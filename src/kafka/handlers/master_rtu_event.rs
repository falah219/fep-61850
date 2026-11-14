//! MASTER_RTU_EVENTS: UPDATE_ID_RTU.

use std::sync::Arc;

use tracing::{info, warn};

use crate::connection::Manager;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::MasterRtuEventMessage;

pub(crate) async fn master_rtu(mgr: &Arc<Manager>, m: MasterRtuEventMessage) {
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
    mgr.stop(m.id_rtu).await;
    if let Err(e) = mgr.reload(new_id, None).await {
        warn!("[UPDATE_ID_RTU] reload RTU {new_id} gagal: {e}");
    }
}
