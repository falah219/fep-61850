//! Bangun `DataMessage` lalu kirim ke RTU_DATAS + DATAPOINTS.
//! Padanan `kafka/publisher/message_builder.rs` di FEP IEC 104 (alur & aturan sama).

use crate::iec61850::acquisition::PointState;
use crate::iec61850::mapper::{quality_flag, quality_group};
use crate::iec61850::mapping::Point;
use crate::kafka::{DataMessage, KafkaProducer, TOPIC_DATAPOINTS, TOPIC_RTU_DATAS};
use crate::resolver::{FormulaResolver, GeneralActionResolver, GroupResolver, TagResolver};

pub struct Publisher {
    pub producer: KafkaProducer,
    pub tags: TagResolver,
    pub formulas: FormulaResolver,
    pub groups: GroupResolver,
    pub general_actions: GeneralActionResolver,
}

impl Publisher {
    /// `entry_time` = waktu entri report (ms UTC), dipakai kalau titik tidak punya atribut `t`.
    pub fn publish(&self, ip: &str, p: &Point, s: &PointState, action: &str, entry_time: Option<i64>) {
        let flag = quality_flag(s.q, s.t.map(|(_, tq)| tq));
        let (time, time_desc) = message_time(s.t, entry_time, || chrono::Utc::now().timestamp_millis());
        // group: master_data_objects.initial = CDC
        let group = self.groups.get_group(&p.cdc).map(sanitize).unwrap_or_default();
        // general_action: master_general_actions (protocol IEC61850), kunci = action
        let general_action = self.general_actions.get_code(action).and_then(|c| c.parse().ok());
        let key = p.id_rtu.to_string();

        let mut message = DataMessage {
            ip_address: ip,
            protocol: "IEC61850",
            id_rtu: p.id_rtu,
            action,
            tag: "",
            value: &s.value,
            address: p.ioa,
            ca: 0,
            asdu: 0,
            typ: &p.cdc,
            quality: quality_group(s.q),
            flag: &flag,
            group: &group,
            general_action,
            time,
            time_desc,
        };
        self.producer.send_data(TOPIC_RTU_DATAS, &key, &message);

        let Some(tag) = self
            .tags
            .get_tag(p.id_rtu, p.ioa)
            .filter(|tag| !self.tags.is_manual_set(tag))
            .map(sanitize)
            .filter(|tag| !tag.is_empty())
        else {
            return;
        };
        let scaled = self.formulas.apply_formula_to_value(&tag, s.value.clone());
        message.tag = &tag;
        message.value = &scaled;
        self.producer.send_data(TOPIC_DATAPOINTS, &key, &message);
    }
}

/// Urutan sumber waktu: atribut `t` -> waktu entri report -> jam server.
/// Waktu IEC 61850 sudah UTC: tidak dikoreksi TIMEZONE seperti waktu RTU IEC 104.
fn message_time(t: Option<(i64, u8)>, entry_time: Option<i64>, now: impl FnOnce() -> i64) -> (i64, &'static str) {
    match (t, entry_time) {
        (Some((ms, _)), _) => (ms, "RTU"),
        (None, Some(ms)) => (ms, "RTU"),
        (None, None) => (now(), "SERVER"),
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

    #[test]
    fn sumber_waktu() {
        let now = || 999;
        assert_eq!(message_time(Some((100, 0)), Some(200), now), (100, "RTU")); // atribut t menang
        assert_eq!(message_time(None, Some(200), now), (200, "RTU")); // waktu entri report
        assert_eq!(message_time(None, None, now), (999, "SERVER"));
    }

    #[test]
    fn sanitize_tag() {
        assert_eq!(sanitize("SIM1_ANIN1".into()), "SIM1_ANIN1");
        assert_eq!(sanitize("A B.C/D".into()), "ABCD");
    }
}
