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

/// `id = None` -> semua RTU; `Some(id)` -> satu RTU (untuk reload dari event Kafka).
pub async fn load_rtus(c: &Client, id: Option<i32>) -> DbResult<Vec<RtuConfig>> {
    let integrity_period_ms = env_u64("INTEGRITY_PERIOD_MS", 60_000);
    let rcb_retry_secs = env_u64("RCB_RETRY_SECS", DEFAULT_RCB_RETRY_SECS).max(1);
    let brcb_resv_tms_secs = env_u64("BRCB_RESV_TMS_SECS", DEFAULT_BRCB_RESV_TMS_SECS).min(i16::MAX as u64) as i16;
    let liveness_secs = env_u64("LIVENESS_SECS", 30).max(1);
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
                liveness_secs,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resv_tms_lebih_pendek_dari_retry_rcb() {
        assert_eq!(DEFAULT_BRCB_RESV_TMS_SECS, 20);
        assert!(DEFAULT_BRCB_RESV_TMS_SECS < DEFAULT_RCB_RETRY_SECS);
    }
}
