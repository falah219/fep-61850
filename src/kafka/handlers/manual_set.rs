//! MANUAL_SET_EVENT -> MANUAL_SET_EVENT_RESPONSE.

use std::sync::Arc;

use tracing::{info, warn};

use crate::connection::Manager;
use crate::kafka::models::{ManualSetEventMessage, ManualSetEventResponse, PROTOCOL};
use crate::kafka::TOPIC_MANUAL_SET_RESPONSE;
use crate::utils::timestamp::now_ms;

/// Pola `ManualSetHandler` IEC 104; respons ke MANUAL_SET_EVENT_RESPONSE dengan key "IEC61850".
/// Beda: IED yang tidak berjalan / tidak dikenal juga dibalas FAILED (pesan protocol lain tidak sampai ke sini).
pub(crate) fn manual_set(mgr: &Arc<Manager>, m: ManualSetEventMessage) {
    if !m.event.eq_ignore_ascii_case("TOGGLE_MANUAL_SET") {
        return warn!("[MANUAL_SET] event tidak dikenal: {}", m.event);
    }
    let result = check(mgr, &m);
    match result {
        Ok(()) => {
            mgr.publisher.tags.set_manual_set(&m.tag, m.manual_set);
            info!("[MANUAL_SET] {} tag={} user={}", if m.manual_set { "STOPPED" } else { "STARTED" }, m.tag, m.user.as_deref().unwrap_or(""));
        }
        Err(e) => warn!("[MANUAL_SET] id_rtu={} tag={}: {e}", m.id_rtu, m.tag),
    }
    mgr.publisher.producer.send_raw(TOPIC_MANUAL_SET_RESPONSE, PROTOCOL, &response(m, result, now_ms()));
}

/// Ok = IED berjalan dan tag dikenal.
fn check(mgr: &Manager, m: &ManualSetEventMessage) -> Result<(), &'static str> {
    if !mgr.contains(m.id_rtu) {
        return Err(if mgr.db_row(m.id_rtu).is_some() { "IED tidak berjalan" } else { "IED tidak dikenal" });
    }
    if mgr.publisher.tags.has_tag(&m.tag) { Ok(()) } else { Err("Datapoint not found") }
}

fn response(m: ManualSetEventMessage, result: Result<(), &str>, timestamp: i64) -> ManualSetEventResponse {
    let ok = result.is_ok();
    ManualSetEventResponse {
        status: if ok { "SUCCESS" } else { "FAILED" }.into(),
        protocol: m.protocol,
        tag: m.tag,
        manual_set: ok.then_some(m.manual_set),
        user: if ok { m.user } else { None },
        error: result.err().map(String::from),
        timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::testing::{fake_ied, manager, remember_db_row, rtu};
    use crate::iec61850::acquisition::Ctl;
    use std::time::Duration;

    fn msg(id_rtu: i32, tag: &str) -> ManualSetEventMessage {
        ManualSetEventMessage {
            event: "TOGGLE_MANUAL_SET".into(),
            protocol: "IEC61850".into(),
            tag: tag.into(),
            manual_set: true,
            id_rtu,
            user: Some("operator1".into()),
        }
    }

    #[tokio::test]
    async fn ied_tidak_berjalan_atau_tidak_dikenal_dibalas_failed() {
        let mgr = manager();
        assert_eq!(check(&mgr, &msg(4242, "T")), Err("IED tidak dikenal"));
        remember_db_row(&mgr, rtu(4243));
        assert_eq!(check(&mgr, &msg(4243, "T")), Err("IED tidak berjalan"));
        fake_ied(&mgr, 9001, |rx| while !matches!(rx.recv(), Ok(Ctl::Stop) | Err(_)) {});
        assert_eq!(check(&mgr, &msg(9001, "TAG_TIDAK_ADA")), Err("Datapoint not found"));
        mgr.publisher.tags.add_tag(9001, 2001, "BAY1_ANIN1_2001".into());
        assert_eq!(check(&mgr, &msg(9001, "BAY1_ANIN1_2001")), Ok(()));

        let r = response(msg(4243, "T"), Err("IED tidak berjalan"), 1);
        assert_eq!((r.status.as_str(), r.error.as_deref(), r.manual_set, r.user), ("FAILED", Some("IED tidak berjalan"), None, None));
        let r = response(msg(9001, "BAY1_ANIN1_2001"), Ok(()), 1);
        assert_eq!((r.status.as_str(), r.error, r.manual_set), ("SUCCESS", None, Some(true)));
        mgr.stop_all(Duration::from_secs(5)).await;
    }
}
