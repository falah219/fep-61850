//! CAPTURE_DATA_CHANGE (CDC tabel konfigurasi, dari iconics_configuration_cdc_listener).
//!
//! `record` = baris LENGKAP setelah perubahan (DELETE: baris yang dihapus), TANPA nilai lama; `data` = bentuk
//! lama (cadangan). Backend sering meng-UPDATE kolom status `iec61850s` (status_rtu, rtu_online_at, ...) saat
//! RTU online/offline, jadi `iec61850s` dibandingkan dengan konfigurasi IED yang sedang dipakai dan hanya
//! di-reload kalau kolom koneksi/akuisisi berubah.

use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::connection::Manager;
use crate::domain::RtuConfig;
use crate::kafka::event_handler::{data_id_rtu, is_ours};
use crate::kafka::models::CaptureDataChangeMessage;
use crate::repository as repo;

/// Tindakan untuk satu pesan CDC (dipisah dari eksekusi supaya bisa diuji tanpa DB).
#[derive(Debug, PartialEq)]
pub(crate) enum Plan {
    Ignore(&'static str),
    ReloadRtu(i32),
    StopRtu(i32),
    /// Reload semua IED + WARN (id_rtu tidak bisa ditentukan)
    ReloadAll(&'static str),
    /// id_rtu dicari dari master_datapoints.id
    RtuOfDatapoint(String),
    Formulas,
    Groups,
    GeneralActions,
    ManualSets,
}

pub(crate) fn plan(m: &CaptureDataChangeMessage, running: impl Fn(i32) -> Option<RtuConfig>) -> Plan {
    if m.schema != "public" {
        return Plan::Ignore("schema bukan public");
    }
    let row = m.row();
    let text = |k: &str| row.and_then(|r| r.get(k)).and_then(Value::as_str);
    match m.table.as_str() {
        "formulas" => Plan::Formulas,
        "manual_sets" => Plan::ManualSets,
        "master_data_objects" | "master_general_actions" if text("protocol").is_some_and(|p| !is_ours(p)) => {
            Plan::Ignore("protocol bukan IEC61850")
        }
        "master_data_objects" => Plan::Groups,
        "master_general_actions" => Plan::GeneralActions,
        "iec61850s" => rtu_plan(&m.operation.to_uppercase(), row, running),
        "iec61850_report_controls" => match data_id_rtu(row) {
            Some(id) => Plan::ReloadRtu(id),
            None => Plan::ReloadAll("iec61850_report_controls tanpa id_rtu"),
        },
        "master_datapoint_iec61850s" | "iec61850_command_formulas" => match text("id_master_datapoint") {
            Some(dp) => Plan::RtuOfDatapoint(dp.to_string()),
            None => Plan::ReloadAll("tanpa id_master_datapoint"),
        },
        _ => Plan::Ignore("tabel tidak relevan"),
    }
}

fn rtu_plan(op: &str, row: Option<&Value>, running: impl Fn(i32) -> Option<RtuConfig>) -> Plan {
    let Some(id) = data_id_rtu(row) else { return Plan::ReloadAll("iec61850s tanpa id_rtu") };
    let flag = |k: &str| row.and_then(|r| r.get(k)).and_then(Value::as_bool);
    if op == "DELETE" || flag("is_deleted") == Some(true) {
        return Plan::StopRtu(id);
    }
    if op == "INSERT" {
        return Plan::ReloadRtu(id);
    }
    match running(id) {
        Some(cfg) if rtu_config_changed(&cfg, row) => Plan::ReloadRtu(id),
        Some(_) => Plan::Ignore("hanya kolom status / non-koneksi yang berubah"),
        None if flag("state_rtu") == Some(false) => Plan::Ignore("IED tidak berjalan dan state_rtu = false"),
        None => Plan::ReloadRtu(id),
    }
}

/// true kalau kolom `iec61850s` yang dipakai `RtuConfig` berbeda dengan konfigurasi yang sedang berjalan.
/// Kolom yang tidak ada di `record` dianggap tidak berubah. Kolom status (status_rtu, rtu_online_at,
/// rtu_offline_at, status_channel, channel_*, created_at, updated_at) dan kolom lain diabaikan.
// ponytail: daftar kolom ditulis tangan, tambahkan di sini kalau RtuConfig membaca kolom DB baru
fn rtu_config_changed(cfg: &RtuConfig, row: Option<&Value>) -> bool {
    let Some(r) = row else { return true };
    let text = |k: &str, cur: &str| r.get(k).is_some_and(|v| v.as_str() != Some(cur));
    // angka di DB dipotong ke >= 0 saat dimuat (load_rtus), jadi bandingkan dengan cara yang sama
    let num = |k: &str, cur: i64, clamp: bool| {
        r.get(k).is_some_and(|v| v.as_i64().map(|x| if clamp { x.max(0) } else { x }) != Some(cur))
    };
    text("ip_address", &cfg.ip_address)
        || num("port", i64::from(cfg.port), false)
        || text("local_ip_address", &cfg.local_ip_address)
        || text("acquisition_mode", &cfg.acquisition_mode)
        || num("poll_interval_ms", cfg.poll_interval_ms as i64, true)
        || num("general_interrogation_interval", cfg.general_interrogation_interval as i64, true)
        || num("connect_timeout_ms", i64::from(cfg.connect_timeout_ms), true)
        || num("request_timeout_ms", i64::from(cfg.request_timeout_ms), true)
        || num("reconnect_time_ms", cfg.reconnect_time_ms as i64, true)
        || num("reconnect_retry_count", i64::from(cfg.reconnect_retry_count), true)
        || r.get("state_rtu").is_some_and(|v| v.as_bool() != Some(cfg.state_rtu))
        || text("ap_title", &cfg.ap_title)
        || num("ae_qualifier", i64::from(cfg.ae_qualifier), false)
        || text("p_selector", &cfg.p_selector)
        || text("s_selector", &cfg.s_selector)
        || text("t_selector", &cfg.t_selector)
}

pub(crate) async fn capture_data_change(mgr: &Arc<Manager>, m: CaptureDataChangeMessage) {
    let (op, table) = (m.operation.as_str(), m.table.as_str());
    match plan(&m, |id| mgr.config_of(id)) {
        Plan::Ignore(why) => debug!("[CDC] {op} {table} diabaikan: {why}"),
        Plan::ReloadRtu(id) => {
            info!(id_rtu = id, "[CDC] {op} {table}: reload IED");
            mgr.reload_later(Some(id), None);
        }
        Plan::StopRtu(id) => {
            info!(id_rtu = id, "[CDC] {op} {table}: hentikan IED");
            mgr.stop(id).await;
        }
        Plan::ReloadAll(why) => {
            warn!("[CDC] {op} {table}: {why}, reload semua IED");
            mgr.reload_later(None, None);
        }
        Plan::RtuOfDatapoint(dp) => {
            let id = match mgr.pool.get().await {
                Ok(c) => repo::rtu_of_datapoint(&c, &dp).await.ok().flatten(),
                Err(_) => None,
            };
            match id {
                Some(id) => {
                    info!(id_rtu = id, "[CDC] {op} {table}: reload IED");
                    mgr.reload_later(Some(id), None);
                }
                None => {
                    warn!("[CDC] {op} {table}: id_rtu datapoint {dp} tidak ditemukan, reload semua IED");
                    mgr.reload_later(None, None);
                }
            }
        }
        Plan::Formulas => reload_formulas(mgr).await,
        Plan::Groups => reload_groups(mgr).await,
        Plan::GeneralActions => reload_general_actions(mgr).await,
        Plan::ManualSets => reload_manual_sets(mgr).await,
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

async fn reload_manual_sets(mgr: &Arc<Manager>) {
    let Ok(c) = mgr.pool.get().await else { return warn!("[CDC] DB tidak tersedia") };
    match repo::load_manual_sets(&c).await {
        Ok(v) => mgr.publisher.tags.load_manual_set(v),
        Err(e) => warn!("[CDC] reload manual set gagal: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::testing::rtu;
    use crate::kafka::payload_examples::example;
    use serde_json::json;

    const ID: i32 = 9001;

    /// Konfigurasi IED 9001 yang sedang berjalan (sama dengan baris contoh docs/examples).
    fn running(id: i32) -> Option<RtuConfig> {
        (id == ID).then(|| RtuConfig { ip_address: "10.10.1.21".into(), ..rtu(ID) })
    }

    fn msg(v: Value) -> CaptureDataChangeMessage {
        serde_json::from_value(v).unwrap()
    }

    /// Contoh `iec61850s` di docs dengan `record` diubah.
    fn rtu_msg(op: &str, change: Value) -> CaptureDataChangeMessage {
        let mut v = example("consume", "CAPTURE_DATA_CHANGE__iec61850s.status_only");
        v["operation"] = op.into();
        for (k, x) in change.as_object().unwrap() {
            v["record"][k] = x.clone();
        }
        msg(v)
    }

    #[test]
    fn sampel_asli_dnp3s_diabaikan() {
        let m = msg(example("consume", "CAPTURE_DATA_CHANGE__dnp3s.real"));
        assert_eq!(plan(&m, running), Plan::Ignore("tabel tidak relevan"));
        assert!(matches!(plan(&msg(json!({"table":"iec104s","schema":"public","operation":"UPDATE","record":{"id_rtu":1}})), running), Plan::Ignore(_)));
    }

    #[test]
    fn update_kolom_status_iec61850s_tidak_reload() {
        assert!(matches!(plan(&rtu_msg("UPDATE", json!({})), running), Plan::Ignore(_)));
        let status = json!({"status_rtu":"green","rtu_online_at":"1794785709999","rtu_offline_at":"1794791384243",
                            "status_channel":true,"channel_number":2,"updated_at":"2026-11-16T02:00:00.000+00:00","name":"GI BARU"});
        assert!(matches!(plan(&rtu_msg("UPDATE", status), running), Plan::Ignore(_)));
    }

    #[test]
    fn update_koneksi_iec61850s_reload_ied_itu_saja() {
        assert_eq!(plan(&rtu_msg("UPDATE", json!({"port": 103})), running), Plan::ReloadRtu(ID));
        for change in [
            json!({"ip_address":"10.10.1.99"}),
            json!({"acquisition_mode":"POLLING"}),
            json!({"poll_interval_ms":500}),
            json!({"state_rtu":false}),
            json!({"t_selector":"00 02"}),
        ] {
            assert_eq!(plan(&rtu_msg("UPDATE", change.clone()), running), Plan::ReloadRtu(ID), "{change}");
        }
        // nilai negatif dimuat sebagai 0 -> sama dengan 0 yang sedang dipakai
        assert!(matches!(plan(&rtu_msg("UPDATE", json!({"general_interrogation_interval":-1})), running), Plan::Ignore(_)));
    }

    #[test]
    fn insert_delete_is_deleted_iec61850s() {
        assert_eq!(plan(&rtu_msg("INSERT", json!({})), running), Plan::ReloadRtu(ID));
        assert_eq!(plan(&rtu_msg("DELETE", json!({})), running), Plan::StopRtu(ID));
        assert_eq!(plan(&rtu_msg("UPDATE", json!({"is_deleted":true})), running), Plan::StopRtu(ID));
        // IED belum berjalan: jalankan kalau aktif, abaikan kalau state_rtu false
        assert_eq!(plan(&rtu_msg("UPDATE", json!({"id_rtu":9005})), running), Plan::ReloadRtu(9005));
        assert!(matches!(plan(&rtu_msg("UPDATE", json!({"id_rtu":9005,"state_rtu":false})), running), Plan::Ignore(_)));
    }

    #[test]
    fn tabel_lain() {
        let m = |table: &str, record: Value| msg(json!({"table":table,"schema":"public","operation":"UPDATE","record":record}));
        let dp = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
        assert_eq!(plan(&m("iec61850_command_formulas", json!({"id_master_datapoint":dp})), running), Plan::RtuOfDatapoint(dp.into()));
        assert_eq!(plan(&m("master_datapoint_iec61850s", json!({"id_master_datapoint":dp})), running), Plan::RtuOfDatapoint(dp.into()));
        assert!(matches!(plan(&m("master_datapoint_iec61850s", json!({})), running), Plan::ReloadAll(_)));
        assert_eq!(plan(&m("iec61850_report_controls", json!({"id_rtu":ID})), running), Plan::ReloadRtu(ID));
        assert!(matches!(plan(&m("master_data_objects", json!({"protocol":"DNP3"})), running), Plan::Ignore(_)));
        assert_eq!(plan(&m("master_data_objects", json!({"protocol":"IEC61850"})), running), Plan::Groups);
        assert_eq!(plan(&m("master_general_actions", json!({})), running), Plan::GeneralActions);
        assert_eq!(plan(&m("formulas", json!({})), running), Plan::Formulas);
        assert_eq!(plan(&m("manual_sets", json!({})), running), Plan::ManualSets);
        // bentuk lama: `data` tetap dibaca
        let old = msg(json!({"table":"iec61850_report_controls","schema":"public","operation":"UPDATE","data":{"id_rtu":ID}}));
        assert_eq!(plan(&old, running), Plan::ReloadRtu(ID));
        let other_schema = msg(json!({"table":"iec61850s","schema":"audit","operation":"UPDATE","record":{"id_rtu":ID}}));
        assert!(matches!(plan(&other_schema, running), Plan::Ignore(_)));
    }

    #[test]
    fn contoh_command_formulas_di_docs() {
        let m = msg(example("consume", "CAPTURE_DATA_CHANGE__iec61850_command_formulas"));
        assert!(matches!(plan(&m, running), Plan::RtuOfDatapoint(_)));
    }
}
