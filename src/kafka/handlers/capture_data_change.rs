//! CAPTURE_DATA_CHANGE (CDC tabel konfigurasi): reload RTU (debounce) / resolver.

use std::sync::Arc;

use serde_json::Value;
use tracing::warn;

use crate::connection::Manager;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::CaptureDataChangeMessage;
use crate::repository as repo;

pub(crate) async fn capture_data_change(mgr: &Arc<Manager>, m: CaptureDataChangeMessage) {
    if m.schema != "public" {
        return;
    }
    let data = m.data.as_ref();
    match m.table.as_str() {
        "formulas" => reload_formulas(mgr).await,
        "master_data_objects" => reload_groups(mgr).await,
        "master_general_actions" => reload_general_actions(mgr).await,
        // Reload digabung per id_rtu (RELOAD_DEBOUNCE_MS): import / edit massal = satu reload.
        "iec61850s" | "iec61850_report_controls" => {
            let state = if m.operation == "DELETE" { Some(false) } else { None };
            mgr.reload_later(data_id_rtu(data), state);
        }
        "master_datapoint_iec61850s" => {
            // id_rtu tidak ada di tabel ekstensi; cari lewat master_datapoints, kalau gagal reload semua.
            let dp = data.and_then(|d| d.get("id_master_datapoint")).and_then(Value::as_str);
            let id = match (dp, mgr.pool.get().await) {
                (Some(dp), Ok(c)) => repo::rtu_of_datapoint(&c, dp).await.ok().flatten(),
                _ => None,
            };
            mgr.reload_later(id, None);
        }
        _ => {}
    }
}

async fn reload_formulas(mgr: &Arc<Manager>) {
    let Ok(c) = mgr.pool.get().await else { return warn!("[CDC] DB tidak tersedia") };
    match repo::load_formulas(&c).await {
        Ok(v) => mgr.publisher.formulas.load(v),
        Err(e) => warn!("[CDC] reload formula gagal: {e}"),
    }
}

pub(crate) async fn reload_groups(mgr: &Arc<Manager>) {
    let Ok(c) = mgr.pool.get().await else { return warn!("[CDC] DB tidak tersedia") };
    match repo::load_groups(&c).await {
        Ok(v) => mgr.publisher.groups.load(v),
        Err(e) => warn!("reload group gagal: {e}"),
    }
}

async fn reload_general_actions(mgr: &Arc<Manager>) {
    let Ok(c) = mgr.pool.get().await else { return warn!("[CDC] DB tidak tersedia") };
    match repo::load_general_actions(&c).await {
        Ok(v) => mgr.publisher.general_actions.load(v),
        Err(e) => warn!("reload general action gagal: {e}"),
    }
}
