//! Mapper IEC 61850 -> field Kafka: nilai (per CDC), quality, flag, reason code.

use serde_json::Value;

use crate::ffi;

// Bit quality IEC 61850-7-3 (sama dengan QUALITY_* di libIEC61850)
const VALIDITY_MASK: u16 = 0b11;
const VALIDITY_INVALID: u16 = ffi::QUALITY_VALIDITY_INVALID as u16;
const VALIDITY_QUESTIONABLE: u16 = ffi::QUALITY_VALIDITY_QUESTIONABLE as u16;
const VALIDITY_RESERVED: u16 = ffi::QUALITY_VALIDITY_RESERVED as u16;

const DETAIL_FLAGS: [(u32, &str); 11] = [
    (ffi::QUALITY_DETAIL_OVERFLOW, "OVERFLOW"),
    (ffi::QUALITY_DETAIL_OUT_OF_RANGE, "OUT_OF_RANGE"),
    (ffi::QUALITY_DETAIL_BAD_REFERENCE, "BAD_REFERENCE"),
    (ffi::QUALITY_DETAIL_OSCILLATORY, "OSCILLATORY"),
    (ffi::QUALITY_DETAIL_FAILURE, "FAILURE"),
    (ffi::QUALITY_DETAIL_OLD_DATA, "OLD_DATA"),
    (ffi::QUALITY_DETAIL_INCONSISTENT, "INCONSISTENT"),
    (ffi::QUALITY_DETAIL_INACCURATE, "INACCURATE"),
    (ffi::QUALITY_SOURCE_SUBSTITUTED, "SUBSTITUTED"),
    (ffi::QUALITY_TEST, "TEST"),
    (ffi::QUALITY_OPERATOR_BLOCKED, "OPERATOR_BLOCKED"),
];

// Bit time quality (byte terakhir UtcTime)
const TQ_CLOCK_FAILURE: u8 = 0x40;
const TQ_CLOCK_NOT_SYNC: u8 = 0x20;

/// Grup quality, urutan disamakan dengan FEP IEC 104:
/// INVALID > SUBSTITUTED > QUESTIONABLE > GOOD.
/// Detail yang setara NON_TOPICAL/BLOCKED/OVERFLOW di IEC 104 (OLD_DATA, OPERATOR_BLOCKED,
/// OVERFLOW) juga dihitung QUESTIONABLE.
pub fn quality_group(q: Option<u16>) -> &'static str {
    let Some(q) = q else { return "GOOD" };
    let has = |bit: u32| u32::from(q) & bit != 0;
    let validity = q & VALIDITY_MASK;
    if validity == VALIDITY_INVALID {
        "INVALID"
    } else if has(ffi::QUALITY_SOURCE_SUBSTITUTED) {
        "SUBSTITUTED"
    } else if validity == VALIDITY_QUESTIONABLE
        || validity == VALIDITY_RESERVED
        || has(ffi::QUALITY_DETAIL_OLD_DATA | ffi::QUALITY_OPERATOR_BLOCKED | ffi::QUALITY_DETAIL_OVERFLOW)
    {
        "QUESTIONABLE"
    } else {
        "GOOD"
    }
}

/// Flag detail dipisah `|` (format sama dengan IEC 104). Kosong = "GOOD".
pub fn quality_flag(q: Option<u16>, time_quality: Option<u8>) -> String {
    let mut flags: Vec<&str> = Vec::new();
    if let Some(q) = q {
        let validity = q & VALIDITY_MASK;
        if validity == VALIDITY_INVALID {
            flags.push("INVALID");
        } else if validity == VALIDITY_QUESTIONABLE {
            flags.push("QUESTIONABLE");
        }
        flags.extend(DETAIL_FLAGS.iter().filter(|(bit, _)| u32::from(q) & bit != 0).map(|(_, n)| *n));
    }
    if time_quality.is_some_and(|tq| tq & (TQ_CLOCK_FAILURE | TQ_CLOCK_NOT_SYNC) != 0) {
        flags.push("CLOCK_NOT_SYNC");
    }
    if flags.is_empty() {
        "GOOD".into()
    } else {
        flags.join("|")
    }
}

/// Reason-for-inclusion (bitmask) -> `action`. Prioritas:
/// GI > INTEGRITY > DATA_CHANGE > QUALITY_CHANGE > DATA_UPDATE.
pub fn reason_action(reason: u32) -> Option<&'static str> {
    [
        (ffi::IEC61850_REASON_GI, "GENERAL_INTERROGATION"),
        (ffi::IEC61850_REASON_INTEGRITY, "INTEGRITY"),
        (ffi::IEC61850_REASON_DATA_CHANGE, "DATA_CHANGE"),
        (ffi::IEC61850_REASON_QUALITY_CHANGE, "QUALITY_CHANGE"),
        (ffi::IEC61850_REASON_DATA_UPDATE, "DATA_UPDATE"),
    ]
    .into_iter()
    .find(|(bit, _)| reason & bit != 0)
    .map(|(_, a)| a)
}

/// Urutan prioritas action (untuk menggabung beberapa anggota dataset milik satu titik).
pub fn action_rank(action: &str) -> u8 {
    match action {
        "GENERAL_INTERROGATION" => 6,
        "INTEGRITY" => 5,
        "DATA_CHANGE" => 4,
        "QUALITY_CHANGE" => 3,
        "DATA_UPDATE" => 2,
        "POLLING" => 1,
        _ => 0,
    }
}

/// Teks TrgOps (`dchg,qchg,dupd,integrity,gi`) -> bitmask. Kosong = None (tidak ditulis ke IED).
pub fn parse_trg_ops(s: &str) -> Result<Option<i32>, String> {
    let mut bits = 0;
    for p in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        bits |= match p {
            "dchg" => ffi::TRG_OPT_DATA_CHANGED,
            "qchg" => ffi::TRG_OPT_QUALITY_CHANGED,
            "dupd" => ffi::TRG_OPT_DATA_UPDATE,
            "integrity" | "period" => ffi::TRG_OPT_INTEGRITY,
            "gi" => ffi::TRG_OPT_GI,
            _ => return Err(format!("trg_ops tidak dikenal: {p}")),
        };
    }
    Ok((bits != 0).then_some(bits as i32))
}

pub fn float_value(v: f64) -> Value {
    serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number)
}

/// MmsValue -> JSON, mengikuti tipe di IEC 104: boolean jadi angka 0/1, DPS jadi 0..3.
///
/// # Safety
/// `v` harus null atau MmsValue valid selama pemanggilan.
pub unsafe fn mms_to_json(v: *const ffi::MmsValue, cdc: &str) -> Value {
    if v.is_null() {
        return Value::Null;
    }
    // SAFETY: v valid (syarat pemanggil); semua fungsi di bawah hanya membaca
    unsafe {
        match ffi::MmsValue_getType(v) {
            ffi::MMS_BOOLEAN => Value::from(i32::from(ffi::MmsValue_getBoolean(v))),
            ffi::MMS_INTEGER => Value::from(ffi::MmsValue_toInt64(v)),
            ffi::MMS_UNSIGNED => Value::from(ffi::MmsValue_toUint32(v)),
            ffi::MMS_FLOAT => float_value(ffi::MmsValue_toDouble(v)),
            // Dbpos: intermediate=0, off=1, on=2, bad=3 (angka sama dengan DPI IEC 104)
            ffi::MMS_BIT_STRING if matches!(cdc, "DPS" | "DPC") => Value::from(ffi::Dbpos_fromMmsValue(v) as i32),
            ffi::MMS_BIT_STRING => Value::from(ffi::MmsValue_getBitStringAsInteger(v)),
            _ => Value::Null,
        }
    }
}

/// Quality dari atribut `q`.
///
/// # Safety
/// `v` harus null atau MmsValue valid.
pub unsafe fn mms_quality(v: *const ffi::MmsValue) -> Option<u16> {
    // SAFETY: syarat pemanggil
    (!v.is_null() && unsafe { ffi::MmsValue_getType(v) } == ffi::MMS_BIT_STRING)
        .then(|| unsafe { ffi::Quality_fromMmsValue(v) })
}

/// Waktu (ms epoch, UTC) + time quality dari atribut `t`. Waktu 0 (1970) dianggap tidak ada.
///
/// # Safety
/// `v` harus null atau MmsValue valid.
pub unsafe fn mms_time(v: *const ffi::MmsValue) -> Option<(i64, u8)> {
    // SAFETY: syarat pemanggil
    unsafe {
        if v.is_null() || ffi::MmsValue_getType(v) != ffi::MMS_UTC_TIME {
            return None;
        }
        let ms = ffi::MmsValue_getUtcTimeInMs(v);
        (ms > 0).then(|| (ms as i64, ffi::MmsValue_getUtcTimeQuality(v)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grup_quality() {
        assert_eq!(quality_group(None), "GOOD");
        assert_eq!(quality_group(Some(0)), "GOOD");
        assert_eq!(quality_group(Some(2)), "INVALID");
        assert_eq!(quality_group(Some(3)), "QUESTIONABLE");
        assert_eq!(quality_group(Some(1024)), "SUBSTITUTED");
        assert_eq!(quality_group(Some(2 | 1024)), "INVALID"); // INVALID menang
        assert_eq!(quality_group(Some(3 | 1024)), "SUBSTITUTED"); // urutan IEC 104
        assert_eq!(quality_group(Some(128)), "QUESTIONABLE"); // OLD_DATA
        assert_eq!(quality_group(Some(4096)), "QUESTIONABLE"); // OPERATOR_BLOCKED
        assert_eq!(quality_group(Some(2048)), "GOOD"); // TEST saja
    }

    #[test]
    fn flag_quality() {
        assert_eq!(quality_flag(None, None), "GOOD");
        assert_eq!(quality_flag(Some(0), Some(0x80)), "GOOD");
        assert_eq!(quality_flag(Some(2 | 64), None), "INVALID|FAILURE");
        assert_eq!(quality_flag(Some(1024 | 2048), None), "SUBSTITUTED|TEST");
        assert_eq!(quality_flag(Some(0), Some(0x20)), "CLOCK_NOT_SYNC");
        assert_eq!(quality_flag(Some(3 | 128), Some(0x40)), "QUESTIONABLE|OLD_DATA|CLOCK_NOT_SYNC");
    }

    #[test]
    fn reason_ke_action() {
        assert_eq!(reason_action(0), None);
        assert_eq!(reason_action(1), Some("DATA_CHANGE"));
        assert_eq!(reason_action(2), Some("QUALITY_CHANGE"));
        assert_eq!(reason_action(4), Some("DATA_UPDATE"));
        assert_eq!(reason_action(8), Some("INTEGRITY"));
        assert_eq!(reason_action(16 | 1), Some("GENERAL_INTERROGATION"));
        assert_eq!(reason_action(32), None);
        assert!(action_rank("GENERAL_INTERROGATION") > action_rank("DATA_CHANGE"));
        assert!(action_rank("DATA_CHANGE") > action_rank("QUALITY_CHANGE"));
    }

    #[test]
    fn trg_ops() {
        assert_eq!(parse_trg_ops("dchg,qchg,integrity,gi"), Ok(Some(1 | 2 | 8 | 16)));
        assert_eq!(parse_trg_ops(""), Ok(None));
        assert!(parse_trg_ops("xyz").is_err());
    }
}
