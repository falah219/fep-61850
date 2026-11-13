//! Handler event Kafka (padanan `kafka/handlers/*` FEP IEC 104).
//!
//! FEP IEC 104 dan FEP IEC 61850 membaca topik yang sama, jadi setiap pesan dengan
//! `protocol` selain "IEC61850" DIABAIKAN.

use std::sync::Arc;

use serde_json::Value;
use tracing::{info, warn};

use crate::iec61850::acquisition::Ctl;
use crate::kafka::consumer::KafkaEvent;
use crate::kafka::models::*;
use crate::kafka::TOPIC_MANUAL_SET_RESPONSE;
use crate::manager::Manager;
use crate::repository as repo;

fn is_ours(protocol: &str) -> bool {
    protocol.eq_ignore_ascii_case(PROTOCOL)
}

fn data_id_rtu(data: Option<&Value>) -> Option<i32> {
    data.and_then(|d| d.get("id_rtu")).and_then(Value::as_i64).map(|v| v as i32)
}

pub async fn handle(mgr: &Arc<Manager>, event: KafkaEvent) {
    match event {
        KafkaEvent::RtuEvent(m) if is_ours(&m.protocol) => rtu_event(mgr, *m).await,
        KafkaEvent::MasterDatapoint(m) if is_ours(&m.protocol) => master_datapoint(mgr, m).await,
        KafkaEvent::MasterRtu(m) if is_ours(&m.protocol) => master_rtu(mgr, m).await,
        KafkaEvent::CommandConfig(m) if is_ours(&m.protocol) => {
            // Sama dengan IEC 104: hanya dicatat.
            info!("[RTU_COMMAND_CONFIG] action={} ip={} ioa={:?} value={:?}", m.action, m.ip_address, m.ioa, m.value);
        }
        KafkaEvent::ManualSet(m) if is_ours(&m.protocol) => manual_set(mgr, m),
        KafkaEvent::CaptureDataChange(m) => capture_data_change(mgr, m).await,
        KafkaEvent::MasterDataObject => reload_groups(mgr).await,
        _ => {}
    }
}

async fn rtu_event(mgr: &Arc<Manager>, m: RtuEventMessage) {
    let id = m.id_rtu.or_else(|| data_id_rtu(m.data.as_ref())).or_else(|| mgr.id_by_ip(&m.ip_address));
    info!("[RTU_EVENTS] event={} ip={} id_rtu={:?}", m.event, safe_ip(&m.ip_address), id);
    let Some(id) = id else {
        return warn!("[RTU_EVENTS] id_rtu tidak ditemukan untuk ip {}", safe_ip(&m.ip_address));
    };
    let result = match m.event.as_str() {
        "UPDATE" | "CREATE" | "ADD" => mgr.reload(id, None).await,
        "DELETE" | "REMOVE" => {
            mgr.stop(id);
            Ok(())
        }
        "STATE" => match m.state_rtu {
            Some(state) => mgr.reload(id, Some(state)).await,
            None => Err("STATE tanpa state_rtu".into()),
        },
        "RESET" => {
            if let Some(ip) = mgr.ip_of(id) {
                mgr.publisher.producer.send_status(&ip, id, 0);
            }
            mgr.reload(id, None).await
        }
        "GI" => {
            if mgr.send(id, Ctl::Gi) {
                Ok(())
            } else {
                Err("IED tidak berjalan".into())
            }
        }
        "CHANNEL" => Ok(()), // sama dengan IEC 104: hanya dicatat
        "COMMAND" => Err("COMMAND belum didukung (fase 9)".into()),
        other => Err(format!("event tidak dikenal: {other}")),
    };
    if let Err(e) = result {
        warn!("[RTU_EVENTS] {} RTU {id}: {e}", m.event);
    }
}

async fn master_datapoint(mgr: &Arc<Manager>, m: MasterDatapointEventMessage) {
    let tags = &mgr.publisher.tags;
    let tag = m.data.as_ref().and_then(|d| d.get("tag")).and_then(Value::as_str);
    match (m.event.as_str(), tag) {
        ("ADD" | "CREATE" | "UPDATE", Some(tag)) => {
            tags.remove_tag(m.id_rtu, m.ioa);
            tags.add_tag(m.id_rtu, m.ioa, tag.to_string());
        }
        ("DELETE", _) => tags.remove_tag(m.id_rtu, m.ioa),
        (ev, _) => return warn!("[MASTER_DATAPOINT] event {ev} tanpa tag / tidak dikenal"),
    }
    info!("[MASTER_DATAPOINT] {} id_rtu={} ioa={}", m.event, m.id_rtu, m.ioa);
    // Mapping (object_reference, FC, ...) bisa ikut berubah: muat ulang IED-nya.
    if mgr.contains(m.id_rtu) {
        if let Err(e) = mgr.reload(m.id_rtu, None).await {
            warn!("[MASTER_DATAPOINT] reload RTU {} gagal: {e}", m.id_rtu);
        }
    }
}

async fn master_rtu(mgr: &Arc<Manager>, m: MasterRtuEventMessage) {
    if m.event != "UPDATE_ID_RTU" {
        return info!("[MASTER_RTU_EVENTS] event tidak dikenal: {}", m.event);
    }
    let Some(new_id) = data_id_rtu(m.new_data.as_ref()) else {
        return warn!("[UPDATE_ID_RTU] new_data.id_rtu tidak ada");
    };
    if new_id == m.id_rtu {
        return;
    }
    info!("[UPDATE_ID_RTU] {} -> {new_id}", m.id_rtu);
    mgr.stop(m.id_rtu);
    if let Err(e) = mgr.reload(new_id, None).await {
        warn!("[UPDATE_ID_RTU] reload RTU {new_id} gagal: {e}");
    }
}

/// Sama dengan `ManualSetHandler` IEC 104; respons ke MANUAL_SET_EVENT_RESPONSE dengan key "IEC61850".
fn manual_set(mgr: &Arc<Manager>, m: ManualSetEventMessage) {
    if !mgr.contains(m.id_rtu) {
        return;
    }
    if !m.event.eq_ignore_ascii_case("TOGGLE_MANUAL_SET") {
        return warn!("[MANUAL_SET] event tidak dikenal: {}", m.event);
    }
    let tags = &mgr.publisher.tags;
    let found = tags.has_tag(&m.tag);
    if found {
        tags.set_manual_set(&m.tag, m.manual_set);
        info!("[MANUAL_SET] {} tag={} user={}", if m.manual_set { "STOPPED" } else { "STARTED" }, m.tag, m.user.as_deref().unwrap_or(""));
    } else {
        warn!("[MANUAL_SET] datapoint tidak ditemukan: {}", m.tag);
    }
    let response = ManualSetEventResponse {
        status: if found { "SUCCESS" } else { "FAILED" }.into(),
        protocol: m.protocol,
        tag: m.tag,
        manual_set: found.then_some(m.manual_set),
        user: if found { m.user } else { None },
        error: (!found).then(|| "Datapoint not found".into()),
        timestamp: chrono::Utc::now().timestamp_millis(),
    };
    mgr.publisher.producer.send_raw(TOPIC_MANUAL_SET_RESPONSE, PROTOCOL, &response);
}

async fn capture_data_change(mgr: &Arc<Manager>, m: CaptureDataChangeMessage) {
    if m.schema != "public" {
        return;
    }
    let data = m.data.as_ref();
    match m.table.as_str() {
        "formulas" => reload_formulas(mgr).await,
        "master_data_objects" => reload_groups(mgr).await,
        "master_general_actions" => reload_general_actions(mgr).await,
        "iec61850s" | "iec61850_report_controls" => match data_id_rtu(data) {
            Some(id) => {
                let state = if m.operation == "DELETE" { Some(false) } else { None };
                if let Err(e) = mgr.reload(id, state).await {
                    warn!("[CDC] reload RTU {id} gagal: {e}");
                }
            }
            None => mgr.reload_all().await,
        },
        "master_datapoint_iec61850s" => {
            // id_rtu tidak ada di tabel ekstensi; cari lewat master_datapoints, kalau gagal reload semua.
            let dp = data.and_then(|d| d.get("id_master_datapoint")).and_then(Value::as_str);
            let id = match (dp, mgr.pool.get().await) {
                (Some(dp), Ok(c)) => repo::rtu_of_datapoint(&c, dp).await.ok().flatten(),
                _ => None,
            };
            match id {
                Some(id) => {
                    if let Err(e) = mgr.reload(id, None).await {
                        warn!("[CDC] reload RTU {id} gagal: {e}");
                    }
                }
                None => mgr.reload_all().await,
            }
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

async fn reload_groups(mgr: &Arc<Manager>) {
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
