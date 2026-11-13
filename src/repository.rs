//! Query konfigurasi dari Postgres (skema: migrations/0001_iec61850.up.sql).

use std::env;

use tokio_postgres::{Client, Row};
use tracing::warn;

use crate::iec61850::client::RcbSettings;
use crate::iec61850::mapper::parse_trg_ops;
use crate::iec61850::mapping::{default_value_attribute, Point, PointMap};

type DbResult<T> = Result<T, String>;

fn db_err(what: &str) -> impl Fn(tokio_postgres::Error) -> String + '_ {
    move |e| format!("{what}: {e}")
}

#[derive(Debug, Clone)]
pub struct RtuConfig {
    pub id_rtu: i32,
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
}

fn non_negative(v: i32) -> u64 {
    v.max(0) as u64
}

pub async fn load_rtus(c: &Client) -> DbResult<Vec<RtuConfig>> {
    let integrity_period_ms = env::var("INTEGRITY_PERIOD_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60_000);
    let rows = c
        .query(
            "SELECT id_rtu, ip_address, port, local_ip_address, acquisition_mode, poll_interval_ms,
                    general_interrogation_interval, connect_timeout_ms, request_timeout_ms,
                    reconnect_time_ms, reconnect_retry_count
             FROM iec61850s WHERE is_deleted = FALSE AND state_rtu = TRUE ORDER BY id_rtu",
            &[],
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
                integrity_period_ms,
            })
        })
        .collect::<Result<_, _>>()
        .map_err(db_err("baris iec61850s"))
}

/// (id_rtu, rcb_reference, settings). Baris dengan trg_ops tidak valid dilewati + warning.
pub async fn load_rcbs(c: &Client) -> DbResult<Vec<(i32, String, RcbSettings)>> {
    let rows = c
        .query(
            "SELECT id_rtu, rcb_reference, dataset_reference, trg_ops, intg_pd_ms, buf_tm_ms
             FROM iec61850_report_controls WHERE enabled = TRUE ORDER BY id",
            &[],
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
    })
}

pub async fn load_points(c: &Client) -> DbResult<PointMap> {
    let rows = c
        .query(
            "SELECT md.id_rtu, md.ioa, md.tag, x.object_reference, x.functional_constraint, x.cdc,
                    x.value_attribute, x.ctl_model, x.sbo_timeout_ms, x.interlock_check, x.synchro_check, x.deadband
             FROM master_datapoint_iec61850s x
             JOIN master_datapoints md ON md.id = x.id_master_datapoint
             JOIN iec61850s r          ON r.id_rtu = md.id_rtu
             WHERE md.is_deleted = FALSE AND r.is_deleted = FALSE AND md.protocol = 'IEC61850'
             ORDER BY md.id_rtu, md.ioa",
            &[],
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
