//! Waktu pesan (padanan `utils/timestamp.rs` FEP IEC 104).

/// Jam server sekarang, ms epoch UTC.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Urutan sumber waktu: atribut `t` -> waktu entri report -> jam server.
/// Waktu IEC 61850 sudah UTC: tidak dikoreksi TIMEZONE seperti waktu RTU IEC 104.
pub fn message_time(t: Option<(i64, u8)>, entry_time: Option<i64>, now: impl FnOnce() -> i64) -> (i64, &'static str) {
    match (t, entry_time) {
        (Some((ms, _)), _) => (ms, "RTU"),
        (None, Some(ms)) => (ms, "RTU"),
        (None, None) => (now(), "SERVER"),
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
}
