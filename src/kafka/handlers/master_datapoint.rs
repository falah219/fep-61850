//! MASTER_DATAPOINT_EVENT: tag berubah + reload mapping (debounce).

use std::sync::Arc;

use serde_json::Value;
use tracing::{info, warn};

use crate::connection::Manager;
use crate::kafka::models::MasterDatapointEventMessage;

pub(crate) async fn master_datapoint(mgr: &Arc<Manager>, m: MasterDatapointEventMessage) {
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
    // Mapping (object_reference, FC, ...) bisa ikut berubah: muat ulang IED-nya (digabung/debounce).
    if mgr.contains(m.id_rtu) {
        mgr.reload_later(Some(m.id_rtu), None);
    }
}
