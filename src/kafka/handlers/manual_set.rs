//! MANUAL_SET_EVENT -> MANUAL_SET_EVENT_RESPONSE.

use std::sync::Arc;

use tracing::{info, warn};

use crate::connection::Manager;
use crate::kafka::models::{ManualSetEventMessage, ManualSetEventResponse, PROTOCOL};
use crate::kafka::TOPIC_MANUAL_SET_RESPONSE;
use crate::utils::timestamp::now_ms;

/// Sama dengan `ManualSetHandler` IEC 104; respons ke MANUAL_SET_EVENT_RESPONSE dengan key "IEC61850".
pub(crate) fn manual_set(mgr: &Arc<Manager>, m: ManualSetEventMessage) {
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
        timestamp: now_ms(),
    };
    mgr.publisher.producer.send_raw(TOPIC_MANUAL_SET_RESPONSE, PROTOCOL, &response);
}
