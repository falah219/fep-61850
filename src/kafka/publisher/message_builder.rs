//! Bangun `DataMessage` lalu kirim ke RTU_DATAS + DATAPOINTS.
//! Padanan `kafka/publisher/message_builder.rs` di FEP IEC 104 (alur & aturan sama).

use crate::domain::{Point, PointState};
use crate::iec61850::mapper::{quality_flag, quality_group};
use crate::kafka::{DataMessage, KafkaProducer, TOPIC_DATAPOINTS, TOPIC_RTU_DATAS};
use crate::resolver::{FormulaResolver, GeneralActionResolver, GroupResolver, TagResolver};
use crate::utils::timestamp::{message_time, now_ms};
use tracing::warn;

/// Asal nilai titik -> field `source` / `rcb` RTU_DATAS & DATAPOINTS. Hanya dua jalur yang mempublish
/// data titik (manual set hanya menyaring DATAPOINTS, tidak membuat pesan sendiri).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Origin<'a> {
    /// Report dari RCB ini (rcb_reference RCB yang menerima report, bukan tebakan dari dataset)
    Report { rcb: &'a str },
    /// Polling, termasuk polling penuh karena event GI dan siklus integrity polling
    Polling,
}

impl<'a> Origin<'a> {
    pub fn source(&self) -> &'static str {
        match self {
            Self::Report { .. } => "REPORT",
            Self::Polling => "POLLING",
        }
    }

    pub fn rcb(&self) -> Option<&'a str> {
        match self {
            Self::Report { rcb } => Some(rcb),
            Self::Polling => None,
        }
    }
}

pub struct Publisher {
    pub producer: KafkaProducer,
    pub tags: TagResolver,
    pub formulas: FormulaResolver,
    pub groups: GroupResolver,
    pub general_actions: GeneralActionResolver,
}

impl Publisher {
    /// Titik termapping -> RTU_DATAS + DATAPOINTS.
    /// `entry_time` = waktu entri report (ms UTC), dipakai kalau titik tidak punya atribut `t`.
    pub fn publish(&self, ip: &str, p: &Point, s: &PointState, action: &str, entry_time: Option<i64>, origin: Origin) {
        self.send(p, self.messages(ip, p, true, s, action, entry_time, origin));
    }

    /// Anggota dataset RCB yang belum punya titik (RTU_DATAS_UNMAPPED, `p` = titik virtual): RTU_DATAS saja.
    pub fn publish_unmapped(&self, ip: &str, p: &Point, s: &PointState, action: &str, entry_time: Option<i64>, origin: Origin) {
        self.send(p, self.messages(ip, p, false, s, action, entry_time, origin));
    }

    fn send(&self, p: &Point, msgs: Vec<(&'static str, String)>) {
        let key = p.id_rtu.to_string();
        for (topic, json) in msgs {
            self.producer.send_with_key(topic, &key, &json);
        }
    }

    /// (topic, JSON) satu titik. `mapped` = false (titik virtual anggota tanpa IOA): hanya RTU_DATAS, `address` null,
    /// `group` "", tanpa DATAPOINTS (tidak ada tag; alarm memakai DATAPOINTS). `object_reference` / `fc` hanya di
    /// RTU_DATAS, DATAPOINTS tidak berubah.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn messages(
        &self,
        ip: &str,
        p: &Point,
        mapped: bool,
        s: &PointState,
        action: &str,
        entry_time: Option<i64>,
        origin: Origin,
    ) -> Vec<(&'static str, String)> {
        let flag = quality_flag(s.q, s.t.map(|(_, tq)| tq));
        let (time, time_desc) = message_time(s.t, entry_time, now_ms);
        // group: master_data_objects.initial = CDC (titik virtual: "")
        let group = if mapped { self.groups.get_group(&p.cdc).map(sanitize).unwrap_or_default() } else { String::new() };
        // general_action: master_general_actions (protocol IEC61850), kunci = action
        let general_action = self.general_actions.get_code(action).and_then(|c| c.parse().ok());

        let mut message = DataMessage {
            ip_address: ip,
            protocol: "IEC61850",
            id_rtu: p.id_rtu,
            action,
            tag: "",
            value: &s.value,
            address: mapped.then_some(p.ioa),
            ca: 0,
            asdu: 0,
            typ: &p.cdc,
            quality: quality_group(s.q),
            flag: &flag,
            group: &group,
            general_action,
            source: origin.source(),
            rcb: origin.rcb(),
            object_reference: Some(p.object_reference.as_str()),
            fc: Some(p.fc.as_str()),
            time,
            time_desc,
        };
        let mut out = Vec::with_capacity(2);
        out.extend(json(TOPIC_RTU_DATAS, &message));
        if !mapped {
            return out;
        }

        let Some(tag) = self
            .tags
            .get_tag(p.id_rtu, p.ioa)
            .filter(|tag| !self.tags.is_manual_set(tag))
            .map(sanitize)
            .filter(|tag| !tag.is_empty())
        else {
            return out;
        };
        let scaled = self.formulas.apply_formula_to_value(&tag, s.value.clone());
        message.tag = &tag;
        message.value = &scaled;
        message.object_reference = None;
        message.fc = None;
        out.extend(json(TOPIC_DATAPOINTS, &message));
        out
    }
}

fn json(topic: &'static str, m: &DataMessage) -> Option<(&'static str, String)> {
    match serde_json::to_string(m) {
        Ok(j) => Some((topic, j)),
        Err(e) => {
            warn!("[KAFKA] Gagal serialize DataMessage: {e}");
            None
        }
    }
}

/// Sama dengan `sanitize` di FEP IEC 104.
fn sanitize(text: String) -> String {
    let valid = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-';
    if text.chars().all(valid) {
        text
    } else {
        text.chars().filter(|c| valid(*c)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (a) report brcbST01 -> REPORT + rcb; (b) polling -> POLLING + null.
    #[test]
    fn origin_ke_source_dan_rcb() {
        let r = Origin::Report { rcb: "SIMIED1LD0/LLN0.BR.brcbST01" };
        assert_eq!((r.source(), r.rcb()), ("REPORT", Some("SIMIED1LD0/LLN0.BR.brcbST01")));
        assert_eq!((Origin::Polling.source(), Origin::Polling.rcb()), ("POLLING", None));
    }

    #[test]
    fn sanitize_tag() {
        assert_eq!(sanitize("SIM1_ANIN1".into()), "SIM1_ANIN1");
        assert_eq!(sanitize("A B.C/D".into()), "ABCD");
    }
}
