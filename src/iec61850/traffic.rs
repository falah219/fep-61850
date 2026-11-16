//! Log lalu lintas data per IED ke RTU_LOGGING (padanan "ASDU RX - Type: ..." FEP IEC 104).
//!
//! - Baris data (REPORT/POLL/GI): hanya kalau `LOG_DATA_TRAFFIC=true`, dibatasi
//!   `LOG_DATA_MAX_PER_SEC` per IED (0 = tanpa batas). Baris yang lewat batas dilewati, lalu di detik
//!   berikutnya dikirim satu baris ringkas.
//! - Baris CONTROL selalu dikirim (audit).
//!
//! Dipanggil juga dari thread report libIEC61850: hanya atomik + format string, tanpa lock.

use std::fmt::Write;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

use crate::domain::CtlVal;
use crate::ffi;

/// Timestamp UTC ISO-8601, format sama dengan `get_timestamp()` FEP IEC 104.
pub fn ts() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

pub struct TrafficLog {
    enabled: bool,
    max_per_sec: u32,
    sec: AtomicU64,
    count: AtomicU32,
    dropped: AtomicU32,
}

impl TrafficLog {
    pub fn new(enabled: bool, max_per_sec: u32) -> Self {
        Self { enabled, max_per_sec, sec: AtomicU64::new(0), count: AtomicU32::new(0), dropped: AtomicU32::new(0) }
    }

    /// Baris yang harus dikirim untuk satu baris data pada detik `now_sec` (epoch):
    /// kosong = dilewati; bisa diawali baris ringkas dari detik sebelumnya.
    /// `line` hanya dibuat kalau baris itu memang dikirim.
    pub fn data_lines(&self, now_sec: u64, line: impl FnOnce() -> String) -> Vec<String> {
        if !self.enabled {
            return Vec::new();
        }
        let mut out = Vec::new();
        if self.max_per_sec > 0 {
            let prev = self.sec.load(Relaxed);
            if prev != now_sec && self.sec.compare_exchange(prev, now_sec, Relaxed, Relaxed).is_ok() {
                self.count.store(0, Relaxed);
                let dropped = self.dropped.swap(0, Relaxed);
                if dropped > 0 {
                    out.push(format!("[INFO] {dropped} log data dilewati (batas LOG_DATA_MAX_PER_SEC)"));
                }
            }
            if self.count.fetch_add(1, Relaxed) >= self.max_per_sec {
                self.dropped.fetch_add(1, Relaxed);
                return out;
            }
        }
        out.push(format!("{} {}", ts(), line()));
        out
    }
}

/// Nama pendek: `LD/LLN0.BR.brcbST01` -> `brcbST01`, `LD/LLN0$DS_ST` -> `DS_ST`.
pub fn short(reference: &str) -> &str {
    reference.rsplit(['.', '$', '/']).next().unwrap_or(reference)
}

/// Gabungan reason-for-inclusion unik, urutan tetap.
pub fn reason_names(mask: u32) -> String {
    let names: Vec<&str> = [
        (ffi::IEC61850_REASON_DATA_CHANGE, "dchg"),
        (ffi::IEC61850_REASON_QUALITY_CHANGE, "qchg"),
        (ffi::IEC61850_REASON_DATA_UPDATE, "dupd"),
        (ffi::IEC61850_REASON_INTEGRITY, "integrity"),
        (ffi::IEC61850_REASON_GI, "gi"),
    ]
    .into_iter()
    .filter(|(bit, _)| mask & bit != 0)
    .map(|(_, n)| n)
    .collect();
    if names.is_empty() { "-".into() } else { names.join(",") }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Report masuk. `entry_id` hanya untuk BRCB. Report reason gi -> baris GI RX.
pub fn report_rx(rcb: &str, dataset: &str, mask: u32, entries: usize, seq: Option<u16>, entry_id: Option<&[u8]>) -> String {
    if mask & ffi::IEC61850_REASON_GI != 0 {
        return format!("GI RX - RCB: {}, Entries: {entries}", short(rcb));
    }
    let seq = seq.map_or_else(|| "-".to_string(), |s| s.to_string());
    let mut s = format!(
        "REPORT RX - RCB: {}, Dataset: {}, Reason: {}, Entries: {entries}, SeqNum: {seq}",
        short(rcb),
        short(dataset),
        reason_names(mask)
    );
    if let Some(id) = entry_id {
        let _ = write!(s, ", EntryID: {}", hex(id));
    }
    s
}

pub fn poll_rx(group: &str, elements: usize) -> String {
    format!("POLL RX - Group: {group}, Elements: {elements}")
}

pub fn gi_tx(rcb: &str) -> String {
    format!("GI TX - RCB: {}", short(rcb))
}

/// ctlVal untuk baris CONTROL TX.
pub fn ctl_val_text(v: CtlVal) -> String {
    match v {
        CtlVal::Bool(b) => b.to_string(),
        CtlVal::Int(i) => i.to_string(),
        CtlVal::Float(f) => f.to_string(),
        CtlVal::Tcmd(0) => "stop".into(),
        CtlVal::Tcmd(1) => "lower".into(),
        CtlVal::Tcmd(_) => "higher".into(),
    }
}

/// `ctl_val` None untuk CANCEL.
pub fn control_tx(action: &str, reference: &str, ctl_val: Option<&str>, or_ident: &str) -> String {
    match ctl_val {
        Some(v) => format!("CONTROL TX - {action} {reference} ctlVal={v} orIdent={or_ident}"),
        None => format!("CONTROL TX - {action} {reference} orIdent={or_ident}"),
    }
}

/// SELECT pada ctlModel direct: tidak ada request ke IED.
pub fn control_skip(action: &str, reference: &str) -> String {
    format!("CONTROL SKIP - {action} {reference} (direct, tidak dikirim)")
}

/// Command ditolak FEP sebelum dikirim (value tidak valid, status-only). Dikirim dengan level ERROR.
pub fn control_not_sent(action: &str, reference: &str, reason: &str) -> String {
    format!("CONTROL {action} {reference} tidak dikirim: {reason}")
}

/// `add_cause` None = positive.
pub fn control_rx(action: &str, reference: &str, add_cause: Option<i32>) -> String {
    match add_cause {
        None => format!("CONTROL RX - {action} {reference} positive"),
        Some(c) => format!("CONTROL RX - {action} {reference} negative AddCause={}", add_cause_name(c)),
    }
}

pub fn termination_rx(reference: &str, positive: bool) -> String {
    format!("CONTROL RX - CommandTermination {reference} {}", if positive { "positive" } else { "negative" })
}

/// Teks LastApplError untuk `msg` RTU_COMMAND: `LastApplError error=1, addCause=10 (blocked-by-interlocking)`.
pub fn appl_error(error: i32, add_cause: i32) -> String {
    format!("LastApplError error={error}, addCause={add_cause} ({})", add_cause_name(add_cause))
}

/// AddCause IEC 61850-7-2 (urutan = enum ControlAddCause libIEC61850).
pub fn add_cause_name(c: i32) -> String {
    const NAMES: [&str; 28] = [
        "unknown",
        "not-supported",
        "blocked-by-switching-hierarchy",
        "select-failed",
        "invalid-position",
        "position-reached",
        "parameter-change-in-execution",
        "step-limit",
        "blocked-by-mode",
        "blocked-by-process",
        "blocked-by-interlocking",
        "blocked-by-synchrocheck",
        "command-already-in-execution",
        "blocked-by-health",
        "1-of-n-control",
        "abortion-by-cancel",
        "time-limit-over",
        "abortion-by-trip",
        "object-not-selected",
        "object-already-selected",
        "no-access-authority",
        "ended-with-overshoot",
        "abortion-due-to-deviation",
        "abortion-by-communication-loss",
        "abortion-by-command",
        "none",
        "inconsistent-parameters",
        "locked-by-other-client",
    ];
    usize::try_from(c).ok().and_then(|i| NAMES.get(i)).map_or_else(|| c.to_string(), |s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DCHG: u32 = ffi::IEC61850_REASON_DATA_CHANGE;
    const QCHG: u32 = ffi::IEC61850_REASON_QUALITY_CHANGE;

    fn body(line: &str) -> &str {
        // "<ts> <isi>": ts ISO-8601 UTC berakhiran Z
        let (t, rest) = line.split_once(' ').unwrap();
        assert!(t.ends_with('Z') && t.contains('T'), "timestamp: {t}");
        rest
    }

    #[test]
    fn format_report() {
        let id = [0x0a, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            report_rx("SIMIED1LD0/LLN0.BR.brcbST01", "SIMIED1LD0/LLN0$DS_ST", DCHG, 2, Some(15), Some(&id)),
            "REPORT RX - RCB: brcbST01, Dataset: DS_ST, Reason: dchg, Entries: 2, SeqNum: 15, EntryID: 0a00000000000000"
        );
        // URCB: tanpa EntryID; reason gabungan
        assert_eq!(
            report_rx("LD/LLN0.RP.urcbMX01", "LD/LLN0.DS_MX", DCHG | QCHG, 3, None, None),
            "REPORT RX - RCB: urcbMX01, Dataset: DS_MX, Reason: dchg,qchg, Entries: 3, SeqNum: -"
        );
        assert_eq!(reason_names(0), "-");
    }

    #[test]
    fn format_gi_poll_control() {
        assert_eq!(gi_tx("LD/LLN0.BR.brcbST01"), "GI TX - RCB: brcbST01");
        assert_eq!(
            report_rx("LD/LLN0.BR.brcbST01", "DS", ffi::IEC61850_REASON_GI, 7, Some(1), None),
            "GI RX - RCB: brcbST01, Entries: 7"
        );
        assert_eq!(poll_rx("LD/GGIO1[MX]", 4), "POLL RX - Group: LD/GGIO1[MX], Elements: 4");
        assert_eq!(
            control_tx("SELECT", "SIMIED1LD0/CSWI1.Pos", Some("false"), "tester"),
            "CONTROL TX - SELECT SIMIED1LD0/CSWI1.Pos ctlVal=false orIdent=tester"
        );
        assert_eq!(control_tx("CANCEL", "X/Y.Pos", None, "op"), "CONTROL TX - CANCEL X/Y.Pos orIdent=op");
        assert_eq!(control_skip("SELECT", "X/Y.Pos"), "CONTROL SKIP - SELECT X/Y.Pos (direct, tidak dikirim)");
        assert_eq!(control_not_sent("OPERATE", "X/Y.Pos", "value DPC harus 1 (OPEN) atau 2 (CLOSE), bukan 0"),
            "CONTROL OPERATE X/Y.Pos tidak dikirim: value DPC harus 1 (OPEN) atau 2 (CLOSE), bukan 0");
        assert_eq!(control_rx("OPERATE", "SIMIED1LD0/CSWI1.Pos", None), "CONTROL RX - OPERATE SIMIED1LD0/CSWI1.Pos positive");
        assert_eq!(
            control_rx("OPERATE", "SIMIED1LD0/CSWI1.Pos", Some(5)),
            "CONTROL RX - OPERATE SIMIED1LD0/CSWI1.Pos negative AddCause=position-reached"
        );
        assert_eq!(termination_rx("SIMIED1LD0/CSWI1.Pos", true), "CONTROL RX - CommandTermination SIMIED1LD0/CSWI1.Pos positive");
        assert_eq!(termination_rx("SIMIED1LD0/CSWI1.Pos", false), "CONTROL RX - CommandTermination SIMIED1LD0/CSWI1.Pos negative");
        assert_eq!(add_cause_name(99), "99");
        assert_eq!(appl_error(1, 10), "LastApplError error=1, addCause=10 (blocked-by-interlocking)");
        assert_eq!(appl_error(0, 99), "LastApplError error=0, addCause=99 (99)");
        assert_eq!(ctl_val_text(CtlVal::Bool(false)), "false");
        assert_eq!(ctl_val_text(CtlVal::Float(12.5)), "12.5");
        assert_eq!(ctl_val_text(CtlVal::Tcmd(2)), "higher");
    }

    #[test]
    fn pembatas_per_detik() {
        let t = TrafficLog::new(true, 2);
        assert_eq!(t.data_lines(100, || "a".into()).len(), 1);
        assert_eq!(body(&t.data_lines(100, || "b".into())[0]), "b");
        // batas tercapai: dilewati, closure tidak dipanggil
        assert!(t.data_lines(100, || panic!("tidak boleh dibuat")).is_empty());
        assert!(t.data_lines(100, || "d".into()).is_empty());
        // detik berikutnya: baris ringkas dulu, lalu baris data
        let lines = t.data_lines(101, || "e".into());
        assert_eq!(lines[0], "[INFO] 2 log data dilewati (batas LOG_DATA_MAX_PER_SEC)");
        assert_eq!(body(&lines[1]), "e");
        assert_eq!(t.data_lines(101, || "f".into()).len(), 1); // ringkas hanya sekali
    }

    #[test]
    fn tanpa_batas_dan_nonaktif() {
        let t = TrafficLog::new(true, 0);
        assert!((0..500).all(|_| t.data_lines(1, || "x".into()).len() == 1));
        // LOG_DATA_TRAFFIC=false: tidak ada baris data (baris CONTROL tidak lewat TrafficLog)
        let off = TrafficLog::new(false, 50);
        assert!(off.data_lines(1, || panic!("tidak boleh dibuat")).is_empty());
    }
}
