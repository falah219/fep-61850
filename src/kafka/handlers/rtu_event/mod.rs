//! RTU_EVENTS: UPDATE / CREATE / DELETE / STATE / RESET / GI / CHANNEL (+ COMMAND di `command.rs`,
//! BROWSE di `browse.rs`).

pub mod browse;
pub mod command;

use std::sync::Arc;

use tracing::{info, warn};

use crate::connection::Manager;
use crate::iec61850::acquisition::Ctl;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::{safe_ip, RtuEventMessage};

pub(crate) async fn rtu_event(mgr: &Arc<Manager>, m: RtuEventMessage) {
    let id = m.id_rtu.or_else(|| data_id_rtu(m.data.as_ref())).or_else(|| mgr.id_by_ip(&m.ip_address));
    info!("[RTU_EVENTS] event={} ip={} id_rtu={:?}", m.event, safe_ip(&m.ip_address), id);
    let Some(id) = id else {
        return warn!("[RTU_EVENTS] id_rtu tidak ditemukan untuk ip {}", safe_ip(&m.ip_address));
    };
    let result = match m.event.as_str() {
        "UPDATE" | "CREATE" | "ADD" => mgr.reload(id, None).await,
        "DELETE" | "REMOVE" => {
            mgr.stop(id).await;
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
        other => Err(format!("event tidak dikenal: {other}").into()),
    };
    if let Err(e) = result {
        warn!("[RTU_EVENTS] {} RTU {id}: {e}", m.event);
    }
}
