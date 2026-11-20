//! Query konfigurasi IED, RCB, dan mapping titik (padanan `repository/rtu_repository.rs` FEP IEC 104).
//! Skema: migrations/0001_iec61850.up.sql.

use std::env;

use tokio_postgres::{Client, Row};
use tracing::warn;

use super::{db_err, DbResult};
use crate::domain::{Point, RtuConfig};
use crate::iec61850::client::{OsiSide, RcbSettings};
use crate::iec61850::mapper::parse_trg_ops;
use crate::iec61850::mapping::{default_value_attribute, PointMap};

fn osi_side_from_env() -> OsiSide {
    let v = env::var("OSI_PARAMS_SIDE").unwrap_or_default();
    OsiSide::parse(&v).unwrap_or_else(|| {
        warn!("OSI_PARAMS_SIDE={v:?} tidak dikenal (none|local|remote), pakai none");
        OsiSide::None
    })
}

/// Default retry RCB (detik).
pub const DEFAULT_RCB_RETRY_SECS: u64 = 30;
/// Default ResvTms BRCB (detik). HARUS < DEFAULT_RCB_RETRY_SECS: kalau reservasi sesi lama yang
/// mati masih menahan BRCB, percobaan ulang RCB berikutnya sudah melewati masa reservasinya.
pub const DEFAULT_BRCB_RESV_TMS_SECS: u64 = 20;

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn non_negative(v: i32) -> u64 {
    v.max(0) as u64
}

/// id_rtu baris `iec61850s` (is_deleted = false) dengan `ip_address` ini (trim + huruf kecil) dan `port` (kalau diisi).
pub async fn ids_by_ip(c: &Client, ip: &str, port: Option<i32>) -> DbResult<Vec<i32>> {
    let rows = c
        .query(
            "SELECT id_rtu FROM iec61850s
             WHERE is_deleted = FALSE AND lower(trim(ip_address)) = lower(trim($1)) AND ($2::INT IS NULL OR port = $2)
             ORDER BY id_rtu",
            &[&ip, &port],
        )
        .await
        .map_err(db_err("query iec61850s per ip"))?;
    Ok(rows.iter().filter_map(|r| r.try_get(0).ok()).collect())
}

/// Baris `iec61850_scl_files` (event SCL_IMPORT, file disimpan backend). Tabel tanpa trigger CDC.
#[derive(Debug, Default)]
pub struct SclFileRow {
    pub file_name: Option<String>,
    pub ied_name: Option<String>,
    pub is_deleted: bool,
    /// `octet_length(content)`
    pub size: Option<i64>,
    /// Hanya diambil kalau `size <= max_bytes` (file terlalu besar tidak ditarik dari DB)
    pub content: Option<String>,
}

/// `content` diasumsikan TEXT (XML apa adanya).
pub async fn load_scl_file(c: &Client, id: &str, id_rtu: i32, max_bytes: i64) -> DbResult<Option<SclFileRow>> {
    let row = c
        .query_opt(
            "SELECT file_name, ied_name, COALESCE(is_deleted, FALSE), octet_length(content)::BIGINT,
                    CASE WHEN octet_length(content) <= $3::BIGINT THEN content END
             FROM iec61850_scl_files WHERE id = $1::TEXT::UUID AND id_rtu = $2::INT",
            &[&id, &id_rtu, &max_bytes],
        )
        .await
        .map_err(db_err("query iec61850_scl_files"))?;
    let Some(r) = row else { return Ok(None) };
    let col = db_err("kolom iec61850_scl_files");
    Ok(Some(SclFileRow {
        file_name: r.try_get(0).map_err(&col)?,
        ied_name: r.try_get(1).map_err(&col)?,
        is_deleted: r.try_get(2).map_err(&col)?,
        size: r.try_get(3).map_err(&col)?,
        content: r.try_get(4).map_err(&col)?,
    }))
}

/// `id = None` -> semua RTU; `Some(id)` -> satu RTU (untuk reload dari event Kafka).
pub async fn load_rtus(c: &Client, id: Option<i32>) -> DbResult<Vec<RtuConfig>> {
    let integrity_period_ms = env_u64("INTEGRITY_PERIOD_MS", 60_000);
    let rcb_retry_secs = env_u64("RCB_RETRY_SECS", DEFAULT_RCB_RETRY_SECS).max(1);
    let brcb_resv_tms_secs = env_u64("BRCB_RESV_TMS_SECS", DEFAULT_BRCB_RESV_TMS_SECS).min(i16::MAX as u64) as i16;
    let liveness_secs = env_u64("LIVENESS_SECS", 30).max(1);
    let oper_timeout_ms = env_u64("OPER_TIMEOUT_MS", 10_000).max(1);
    let dpc_close_value = crate::domain::dpc_close_value(std::env::var("DPC_CLOSE_VALUE").ok().as_deref());
    let control_or_cat = control_or_cat(env::var("CONTROL_OR_CAT").ok().as_deref());
    let flag = |k: &str| crate::logger::env_flag(env::var(k).ok().as_deref(), false);
    let (precheck_position, control_debug) = (flag("CONTROL_PRECHECK_POSITION"), flag("CONTROL_DEBUG"));
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
                liveness_secs,
                oper_timeout_ms,
                dpc_close_value,
                control_or_cat,
                precheck_position,
                control_debug,
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
/// (id_rtu, ioa) datapoint IEC61850 yang tidak punya baris master_datapoint_iec61850s.
pub async fn load_unmapped_ioas(c: &Client, id: Option<i32>) -> DbResult<Vec<(i32, i32)>> {
    let rows = c
        .query(
            "SELECT md.id_rtu, md.ioa FROM master_datapoints md
             WHERE md.protocol = 'IEC61850' AND md.is_deleted = FALSE
               AND ($1::INT IS NULL OR md.id_rtu = $1)
               AND NOT EXISTS (SELECT 1 FROM master_datapoint_iec61850s x WHERE x.id_master_datapoint = md.id)
             ORDER BY md.id_rtu, md.ioa",
            &[&id],
        )
        .await
        .map_err(db_err("query datapoint tanpa mapping"))?;
    let out: Vec<(i32, i32)> = rows.iter().filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?))).collect();
    for (id_rtu, ioa) in &out {
        warn!(id_rtu, ioa, "datapoint IEC61850 tanpa baris master_datapoint_iec61850s, dilewati");
    }
    Ok(out)
}

pub async fn load_rcbs(c: &Client, id: Option<i32>) -> DbResult<Vec<(i32, String, RcbSettings)>> {
    let rows = c
        .query(
            "SELECT id_rtu, rcb_reference, dataset_reference, trg_ops, intg_pd_ms, buf_tm_ms
             FROM iec61850_report_controls
             WHERE enabled = TRUE AND ($1::INT IS NULL OR id_rtu = $1) ORDER BY created_at, rcb_reference",
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
        interlock_check: row.try_get(8)?,
        synchro_check: row.try_get(9)?,
        deadband: row.try_get(10)?,
        or_cat: None,
    })
}

/// Env CONTROL_OR_CAT (0..=8, IEC 61850-7-3 orCategory). Default / tidak valid = 3 (remote-control).
pub fn control_or_cat(env: Option<&str>) -> i32 {
    match env.map(str::trim).filter(|v| !v.is_empty()) {
        None => 3,
        Some(v) => match v.parse::<i32>() {
            Ok(n) if (0..=8).contains(&n) => n,
            _ => {
                warn!("CONTROL_OR_CAT={v:?} tidak valid (0..8), pakai 3 (remote-control)");
                3
            }
        },
    }
}

/// Override orCat per titik dari kolom opsional `iec61850_command_formulas.or_cat`
/// (migrations/0008_iec61850_command_formulas_or_cat.sql). Kolom belum ada -> tanpa override.
async fn load_or_cat_overrides(c: &Client, id: Option<i32>) -> std::collections::HashMap<(i32, i32), i32> {
    let rows = c
        .query(
            "SELECT md.id_rtu, md.ioa, cf.or_cat::INT FROM iec61850_command_formulas cf
             JOIN master_datapoints md ON md.id = cf.id_master_datapoint
             WHERE cf.is_deleted = FALSE AND md.is_deleted = FALSE AND cf.or_cat IS NOT NULL
               AND ($1::INT IS NULL OR md.id_rtu = $1)",
            &[&id],
        )
        .await;
    match rows {
        Ok(rows) => rows.iter().filter_map(|r| Some(((r.try_get(0).ok()?, r.try_get(1).ok()?), r.try_get(2).ok()?))).collect(),
        Err(e) => {
            tracing::debug!("override or_cat per titik tidak dipakai (kolom iec61850_command_formulas.or_cat belum ada?): {e}");
            std::collections::HashMap::new()
        }
    }
}

pub async fn load_points(c: &Client, id: Option<i32>) -> DbResult<PointMap> {
    let rows = c
        .query(
            "SELECT md.id_rtu, md.ioa, md.tag, x.object_reference, x.functional_constraint, x.cdc,
                    x.value_attribute, cf.ctl_model, COALESCE(cf.interlock_check, FALSE),
                    COALESCE(cf.synchro_check, FALSE), x.deadband
             FROM master_datapoint_iec61850s x
             JOIN master_datapoints md ON md.id = x.id_master_datapoint
             LEFT JOIN iec61850_command_formulas cf
                    ON cf.id_master_datapoint = x.id_master_datapoint AND cf.is_deleted = FALSE
             JOIN iec61850s r          ON r.id_rtu = md.id_rtu
             WHERE md.is_deleted = FALSE AND r.is_deleted = FALSE AND md.protocol = 'IEC61850'
               AND ($1::INT IS NULL OR md.id_rtu = $1)
             ORDER BY md.id_rtu, md.ioa",
            &[&id],
        )
        .await
        .map_err(db_err("query mapping titik"))?;
    let or_cats = load_or_cat_overrides(c, id).await;
    let mut map = PointMap::default();
    for row in &rows {
        match to_point(row).map(|p| Point { or_cat: or_cats.get(&(p.id_rtu, p.ioa)).copied(), ..p }) {
            Ok(p) if p.object_reference.trim().is_empty() => {
                warn!(id_rtu = p.id_rtu, ioa = p.ioa, "datapoint IEC61850 tanpa object_reference, dilewati");
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn or_cat_dari_env() {
        assert_eq!(control_or_cat(None), 3);
        assert_eq!(control_or_cat(Some(" 2 ")), 2);
        assert_eq!(control_or_cat(Some("0")), 0);
        assert_eq!(control_or_cat(Some("9")), 3);
        assert_eq!(control_or_cat(Some("x")), 3);
    }

    #[test]
    fn resv_tms_lebih_pendek_dari_retry_rcb() {
        assert_eq!(DEFAULT_BRCB_RESV_TMS_SECS, 20);
        assert!(DEFAULT_BRCB_RESV_TMS_SECS < DEFAULT_RCB_RETRY_SECS);
    }
}
