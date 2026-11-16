//! RTU_EVENTS: UPDATE / CREATE / DELETE / STATE / RESET / GI / CHANNEL (+ COMMAND di `command.rs`,
//! BROWSE di `browse.rs`).

pub mod browse;
pub mod command;

use std::sync::Arc;

use serde_json::Value;
use tracing::{info, warn};

use crate::connection::Manager;
use crate::iec61850::acquisition::Ctl;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::{safe_ip, RtuEventMessage};

pub(crate) async fn rtu_event(mgr: &Arc<Manager>, m: RtuEventMessage) {
    let mut id = m.id_rtu.or_else(|| data_id_rtu(m.data.as_ref())).or_else(|| mgr.id_by_ip(&m.ip_address));
    // CREATE/ADD/UPDATE untuk IED yang belum berjalan: cari di tabel iec61850s (backend sebaiknya kirim id_rtu)
    if id.is_none() && matches!(m.event.as_str(), "UPDATE" | "CREATE" | "ADD") {
        let port = m.port.or_else(|| m.data.as_ref().and_then(|d| d.get("port")).and_then(Value::as_i64).and_then(|p| i32::try_from(p).ok()));
        match id_from_db(mgr, &m.ip_address, port).await {
            Ok(found) => id = Some(found),
            Err(e) => return warn!("[RTU_EVENTS] {} tanpa id_rtu: {e}", m.event),
        }
    }
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
            // idempoten: CDC state_rtu untuk perubahan yang sama tidak menjalankan/menghentikan lagi
            Some(state) => mgr.set_state(id, state).await,
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

/// id_rtu dari tabel iec61850s per ip_address (+ port kalau ada).
async fn id_from_db(mgr: &Manager, ip: &str, port: Option<i32>) -> Result<i32, String> {
    if ip.trim().is_empty() {
        return Err("ip_address kosong".into());
    }
    let c = mgr.pool.get().await.map_err(|e| format!("DB tidak tersedia: {e}"))?;
    let ids = crate::repository::ids_by_ip(&c, ip, port).await?;
    pick_one(ip, port, &ids)
}

/// Tepat satu baris cocok -> id_rtu itu. Lebih dari satu -> tidak dipilih sembarangan.
fn pick_one(ip: &str, port: Option<i32>, ids: &[i32]) -> Result<i32, String> {
    match ids {
        [id] => Ok(*id),
        [] => Err(format!("tidak ada baris iec61850s untuk ip {ip} port {port:?}")),
        _ => Err(format!("{} baris iec61850s cocok untuk ip {ip} port {port:?} (id_rtu {ids:?}), tidak dipilih; kirim id_rtu", ids.len())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_rtu_dari_db_harus_tepat_satu() {
        assert_eq!(pick_one("ied-sim-1", None, &[9001]), Ok(9001));
        assert!(pick_one("ied-sim-1", None, &[]).unwrap_err().contains("tidak ada baris"));
        let e = pick_one("10.10.1.21", None, &[9001, 9002]).unwrap_err();
        assert!(e.contains("2 baris") && e.contains("kirim id_rtu"), "{e}");
        assert_eq!(pick_one("10.10.1.21", Some(103), &[9002]), Ok(9002));
    }
}
