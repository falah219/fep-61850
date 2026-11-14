//! Pembagi event Kafka ke handler (padanan `kafka/event_handler.rs` FEP IEC 104).
//!
//! FEP IEC 104 dan FEP IEC 61850 membaca topik yang sama, jadi setiap pesan dengan
//! `protocol` selain "IEC61850" DIABAIKAN.

use std::sync::Arc;

use serde_json::Value;
use tracing::info;

use super::consumer::KafkaEvent;
use super::handlers::capture_data_change::{capture_data_change, reload_groups};
use super::handlers::manual_set::manual_set;
use super::handlers::master_datapoint::master_datapoint;
use super::handlers::master_rtu_event::master_rtu;
use super::handlers::rtu_event::{command, rtu_event};
use super::models::PROTOCOL;
use crate::connection::Manager;

pub(crate) fn is_ours(protocol: &str) -> bool {
    protocol.eq_ignore_ascii_case(PROTOCOL)
}

pub(crate) fn data_id_rtu(data: Option<&Value>) -> Option<i32> {
    data.and_then(|d| d.get("id_rtu")).and_then(Value::as_i64).map(|v| v as i32)
}

pub async fn handle(mgr: &Arc<Manager>, event: KafkaEvent) {
    match event {
        // COMMAND masuk antrean IED SEKARANG (urutan kedatangan terjaga: SELECT sebelum OPERATE).
        // Hanya menunggu hasil (bisa sampai 30 detik) + kirim respons yang jalan di task sendiri,
        // supaya event lain tidak tertahan.
        KafkaEvent::RtuEvent(m) if is_ours(&m.protocol) && m.event == "COMMAND" => {
            if let Some(wait_and_respond) = command::submit(mgr, *m) {
                tokio::spawn(wait_and_respond);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iec61850::acquisition::Ctl;
    use crate::kafka::models::RtuEventMessage;
    use crate::connection::testing::{fake_ied, manager};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn command_tidak_menahan_event_lain() {
        let mgr = manager();
        let (seen_tx, seen_rx) = mpsc::channel();
        // IED palsu: menerima command tapi tidak pernah menjawab (seperti menunggu CommandTermination).
        fake_ied(&mgr, 9001, move |rx| {
            let mut held = Vec::new();
            while let Ok(ctl) = rx.recv() {
                match ctl {
                    Ctl::Command(job) => {
                        let _ = seen_tx.send(job.req.ioa);
                        held.push(job);
                    }
                    Ctl::Stop => break,
                    _ => {}
                }
            }
        });
        let m: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-9001",
                "data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"id_rtu":9001}}"#,
        )
        .unwrap();
        let t = Instant::now();
        handle(&mgr, KafkaEvent::RtuEvent(Box::new(m))).await;
        // Tanpa tokio::spawn ini tertahan sampai COMMAND_TIMEOUT (30 detik).
        assert!(t.elapsed() < Duration::from_millis(500));
        // Command tetap sampai ke thread IED.
        let ioa = tokio::task::spawn_blocking(move || seen_rx.recv_timeout(Duration::from_secs(2))).await.unwrap();
        assert_eq!(ioa, Ok(3001));
        mgr.stop_all(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn command_masuk_antrean_ied_sesuai_urutan_kedatangan() {
        use crate::domain::CommandAction;
        let mgr = manager();
        let (seen_tx, seen_rx) = mpsc::channel();
        fake_ied(&mgr, 9001, move |rx| {
            let mut held = Vec::new();
            while let Ok(ctl) = rx.recv() {
                match ctl {
                    Ctl::Command(job) => {
                        let _ = seen_tx.send(job.req.action);
                        held.push(job);
                    }
                    Ctl::Stop => break,
                    _ => {}
                }
            }
        });
        let cmd = |action: &str| {
            let json = format!(
                r#"{{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-9001",
                    "data":{{"action":"{action}","ioa":3001,"value":1,"id_rtu":9001}}}}"#
            );
            KafkaEvent::RtuEvent(Box::new(serde_json::from_str::<RtuEventMessage>(&json).unwrap()))
        };
        handle(&mgr, cmd("SELECT_RTU")).await;
        handle(&mgr, cmd("OPERATE_RTU")).await;
        // recv BLOCKING di thread runtime (current_thread): task yang di-spawn TIDAK bisa jalan di sini.
        // Jadi command harus sudah masuk antrean secara sinkron di dalam handle().
        let order: Vec<_> = (0..2).map(|_| seen_rx.recv_timeout(Duration::from_secs(1))).collect();
        assert_eq!(order, vec![Ok(CommandAction::Select), Ok(CommandAction::Operate)]);
        mgr.stop_all(Duration::from_secs(5)).await;
    }
}
