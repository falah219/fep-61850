//! Query konfigurasi dari Postgres (skema: migrations/0001_iec61850.up.sql).

use std::env;

use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::{Client, NoTls, Row};
use tracing::warn;

use crate::config::DatabaseConfig;

use crate::iec61850::client::{OsiSide, RcbSettings};
use crate::iec61850::mapper::parse_trg_ops;
use crate::iec61850::mapping::{default_value_attribute, Point, PointMap};

type DbResult<T> = Result<T, String>;

/// Pool koneksi Postgres (sama seperti `repository/pool.rs` FEP IEC 104).
pub fn create_pool(db: &DatabaseConfig) -> DbResult<Pool> {
    let mut cfg = Config::new();
    cfg.host = Some(db.host.clone());
    cfg.port = Some(db.port);
    cfg.user = Some(db.user.clone());
    cfg.password = Some(db.password.clone());
    cfg.dbname = Some(db.dbname.clone());
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).map_err(|e| format!("gagal membuat pool DB: {e}"))
}

fn db_err(what: &str) -> impl Fn(tokio_postgres::Error) -> String + '_ {
    move |e| format!("{what}: {e}")
}

#[derive(Debug, Clone)]
pub struct RtuConfig {
    pub id_rtu: i32,
    pub state_rtu: bool,
    pub channel_number: i32,
    pub ip_address: String,
    pub port: i32,
    pub local_ip_address: String,
    pub acquisition_mode: String,
    pub poll_interval_ms: u64,
    pub general_interrogation_interval: u64,
    pub connect_timeout_ms: u32,
    pub request_timeout_ms: u32,
    pub reconnect_time_ms: u64,
    /// 0 = tanpa batas
    pub reconnect_retry_count: u32,
    /// Tidak ada di DB: kirim ulang semua nilai polling tiap periode ini (env INTEGRITY_PERIOD_MS).
    pub integrity_period_ms: u64,
    /// Tidak ada di DB: jeda coba ulang RCB yang gagal di-setup (env RCB_RETRY_SECS).
    pub rcb_retry_secs: u64,
    /// Tidak ada di DB: ResvTms BRCB (env BRCB_RESV_TMS_SECS)
    pub brcb_resv_tms_secs: i16,
    /// Tidak ada di DB: default tunggu CommandTermination (env OPER_TIMEOUT_MS)
    pub oper_timeout_ms: u64,
    /// Tidak ada di DB: env OSI_PARAMS_SIDE
    pub osi_side: OsiSide,
    pub ap_title: String,
    pub ae_qualifier: i32,
    pub p_selector: String,
    pub s_selector: String,
    pub t_selector: String,
}

fn osi_side_from_env() -> OsiSide {
    let v = env::var("OSI_PARAMS_SIDE").unwrap_or_default();
    OsiSide::parse(&v).unwrap_or_else(|| {
        warn!("OSI_PARAMS_SIDE={v:?} tidak dikenal (none|local|remote), pakai none");
        OsiSide::None
    })
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn non_negative(v: i32) -> u64 {
    v.max(0) as u64
}

/// `id = None` -> semua RTU; `Some(id)` -> satu RTU (untuk reload dari event Kafka).
pub async fn load_rtus(c: &Client, id: Option<i32>) -> DbResult<Vec<RtuConfig>> {
    let integrity_period_ms = env_u64("INTEGRITY_PERIOD_MS", 60_000);
    let rcb_retry_secs = env_u64("RCB_RETRY_SECS", 30).max(1);
    let brcb_resv_tms_secs = env_u64("BRCB_RESV_TMS_SECS", 60).min(i16::MAX as u64) as i16;
    let oper_timeout_ms = env_u64("OPER_TIMEOUT_MS", 10_000).max(1);
    let osi_side = osi_side_from_env();
    let rows = c
        .query(
            "SELECT id_rtu, ip_address, port, local_ip_address, acquisition_mode, poll_interval_ms,
                    general_interrogation_interval, connect_timeout_ms, request_timeout_ms,
                    reconnect_time_ms, reconnect_retry_count, state_rtu, channel_number,
                    ap_title, ae_qualifier, p_selector, s_selector, t_selector
             FROM iec61850s WHERE is_deleted = FALSE AND ($1::INT IS NULL OR id_rtu = $1) ORDER BY id_rtu",
            &[&id],
        )
        .await
        .map_err(db_err("query iec61850s"))?;
    rows.iter()
        .map(|r| -> Result<RtuConfig, tokio_postgres::Error> {
            Ok(RtuConfig {
                id_rtu: r.try_get(0)?,
                ip_address: r.try_get(1)?,
                port: r.try_get(2)?,
                local_ip_address: r.try_get(3)?,
                acquisition_mode: r.try_get(4)?,
                poll_interval_ms: non_negative(r.try_get(5)?),
                general_interrogation_interval: non_negative(r.try_get(6)?),
                connect_timeout_ms: non_negative(r.try_get(7)?) as u32,
                request_timeout_ms: non_negative(r.try_get(8)?) as u32,
                reconnect_time_ms: non_negative(r.try_get(9)?),
                reconnect_retry_count: non_negative(r.try_get(10)?) as u32,
                state_rtu: r.try_get(11)?,
                channel_number: r.try_get(12)?,
                integrity_period_ms,
                rcb_retry_secs,
                brcb_resv_tms_secs,
                oper_timeout_ms,
                osi_side,
                ap_title: r.try_get(13)?,
                ae_qualifier: r.try_get(14)?,
                p_selector: r.try_get(15)?,
                s_selector: r.try_get(16)?,
                t_selector: r.try_get(17)?,
            })
        })
        .collect::<Result<_, _>>()
        .map_err(db_err("baris iec61850s"))
}

/// (id_rtu, rcb_reference, settings). Baris dengan trg_ops tidak valid dilewati + warning.
pub async fn load_rcbs(c: &Client, id: Option<i32>) -> DbResult<Vec<(i32, String, RcbSettings)>> {
    let rows = c
        .query(
            "SELECT id_rtu, rcb_reference, dataset_reference, trg_ops, intg_pd_ms, buf_tm_ms
             FROM iec61850_report_controls
             WHERE enabled = TRUE AND ($1::INT IS NULL OR id_rtu = $1) ORDER BY id",
            &[&id],
        )
        .await
        .map_err(db_err("query iec61850_report_controls"))?;
    let mut out = Vec::new();
    for r in &rows {
        match rcb_row(r) {
            Ok(v) => out.push(v),
            Err(e) => warn!("baris iec61850_report_controls dilewati: {e}"),
        }
    }
    Ok(out)
}

fn rcb_row(r: &Row) -> Result<(i32, String, RcbSettings), String> {
    let get_ms = |i: usize| -> Result<Option<u32>, String> {
        let v: Option<i32> = r.try_get(i).map_err(|e| e.to_string())?;
        Ok(v.map(|v| v.max(0) as u32))
    };
    let trg: Option<String> = r.try_get(3).map_err(|e| e.to_string())?;
    Ok((
        r.try_get(0).map_err(|e| e.to_string())?,
        r.try_get(1).map_err(|e| e.to_string())?,
        RcbSettings {
            dataset: r.try_get(2).map_err(|e| e.to_string())?,
            trg_ops: trg.as_deref().map(parse_trg_ops).transpose()?.flatten(),
            intg_pd_ms: get_ms(4)?,
            buf_tm_ms: get_ms(5)?,
        },
    ))
}

fn to_point(row: &Row) -> Result<Point, tokio_postgres::Error> {
    let cdc: String = row.try_get(5)?;
    let value_attribute: Option<String> = row.try_get(6)?;
    let value_attribute = value_attribute
        .or_else(|| default_value_attribute(&cdc).map(String::from))
        .unwrap_or_default();
    Ok(Point {
        id_rtu: row.try_get(0)?,
        ioa: row.try_get(1)?,
        tag: row.try_get(2)?,
        object_reference: row.try_get(3)?,
        fc: row.try_get(4)?,
        cdc,
        value_attribute,
        ctl_model: row.try_get(7)?,
        sbo_timeout_ms: row.try_get(8)?,
        interlock_check: row.try_get(9)?,
        synchro_check: row.try_get(10)?,
        deadband: row.try_get(11)?,
        oper_timeout_ms: row.try_get(12)?,
    })
}

pub async fn load_points(c: &Client, id: Option<i32>) -> DbResult<PointMap> {
    let rows = c
        .query(
            "SELECT md.id_rtu, md.ioa, md.tag, x.object_reference, x.functional_constraint, x.cdc,
                    x.value_attribute, x.ctl_model, x.sbo_timeout_ms, x.interlock_check, x.synchro_check, x.deadband,
                    x.oper_timeout_ms
             FROM master_datapoint_iec61850s x
             JOIN master_datapoints md ON md.id = x.id_master_datapoint
             JOIN iec61850s r          ON r.id_rtu = md.id_rtu
             WHERE md.is_deleted = FALSE AND r.is_deleted = FALSE AND md.protocol = 'IEC61850'
               AND ($1::INT IS NULL OR md.id_rtu = $1)
             ORDER BY md.id_rtu, md.ioa",
            &[&id],
        )
        .await
        .map_err(db_err("query mapping titik"))?;
    let mut map = PointMap::default();
    for row in &rows {
        match to_point(row) {
            Ok(p) => {
                if let Err(e) = map.insert(p) {
                    warn!("mapping dilewati: {e}");
                }
            }
            Err(e) => warn!("baris mapping tidak valid: {e}"),
        }
    }
    Ok(map)
}

// ---- Query resolver: sama dengan FEP IEC 104, tetapi protocol = 'IEC61850' ----

pub async fn load_tags(c: &Client) -> DbResult<Vec<(i32, i32, String)>> {
    let rows = c
        .query(
            "SELECT id_rtu, ioa, tag FROM master_datapoints
             WHERE is_deleted = FALSE AND protocol = 'IEC61850' AND tag IS NOT NULL AND tag != ''",
            &[],
        )
        .await
        .map_err(db_err("query tag"))?;
    Ok(rows
        .iter()
        .filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?, r.try_get(2).ok()?)))
        .collect())
}

pub async fn load_manual_sets(c: &Client) -> DbResult<Vec<String>> {
    let rows = c
        .query(
            "SELECT md.tag FROM manual_sets ms JOIN master_datapoints md ON md.id = ms.id_master_datapoint
             WHERE ms.is_manual_set = TRUE AND ms.is_deleted = FALSE AND md.is_deleted = FALSE
               AND md.protocol = 'IEC61850' AND md.tag IS NOT NULL",
            &[],
        )
        .await
        .map_err(db_err("query manual set"))?;
    Ok(rows.iter().filter_map(|r| r.try_get(0).ok()).collect())
}

pub async fn load_formulas(c: &Client) -> DbResult<Vec<(String, String, f64, f64, f64, f64)>> {
    let rows = c
        .query(
            "SELECT md.tag, COALESCE(f.scalling_type, 'No Scale'), COALESCE(f.scale, 1.0), COALESCE(f.offset, 0.0),
                    COALESCE(f.span_low, 0.0), COALESCE(f.span_high, 100.0)
             FROM master_datapoints md JOIN formulas f ON md.id = f.id_datapoint
             WHERE md.is_deleted = FALSE AND f.is_deleted = FALSE AND md.protocol = 'IEC61850'
               AND md.tag IS NOT NULL AND md.tag != ''",
            &[],
        )
        .await
        .map_err(db_err("query formula"))?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            Some((
                r.try_get(0).ok()?,
                r.try_get(1).ok()?,
                r.try_get(2).ok()?,
                r.try_get(3).ok()?,
                r.try_get(4).ok()?,
                r.try_get(5).ok()?,
            ))
        })
        .collect())
}

pub async fn load_groups(c: &Client) -> DbResult<Vec<(String, String)>> {
    let rows = c
        .query(
            "SELECT initial, id_group::TEXT FROM master_data_objects
             WHERE is_deleted = FALSE AND initial IS NOT NULL AND id_group IS NOT NULL",
            &[],
        )
        .await
        .map_err(db_err("query group"))?;
    Ok(rows.iter().filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?))).collect())
}

pub async fn load_general_actions(c: &Client) -> DbResult<Vec<(String, i32)>> {
    let rows = c
        .query(
            "SELECT name_action, code FROM master_general_actions
             WHERE protocol = 'IEC61850' AND is_deleted = FALSE AND name_action IS NOT NULL AND name_action != ''",
            &[],
        )
        .await
        .map_err(db_err("query general action"))?;
    Ok(rows.iter().filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?))).collect())
}

/// id_rtu pemilik sebuah master_datapoints.id (uuid dalam bentuk teks).
pub async fn rtu_of_datapoint(c: &Client, id_master_datapoint: &str) -> DbResult<Option<i32>> {
    let row = c
        .query_opt("SELECT id_rtu FROM master_datapoints WHERE id::TEXT = $1", &[&id_master_datapoint])
        .await
        .map_err(db_err("query id_rtu datapoint"))?;
    Ok(row.and_then(|r| r.try_get(0).ok()))
}
