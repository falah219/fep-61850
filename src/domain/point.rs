//! Titik IEC 61850 yang dimapping ke IOA virtual + nilai terakhirnya.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub id_rtu: i32,
    pub ioa: i32,
    pub tag: Option<String>,
    /// Level Data Object, contoh `BAY1LD0/XCBR1.Pos`
    pub object_reference: String,
    pub fc: String,
    pub cdc: String,
    /// Sudah di-resolve: dari DB, atau default dari CDC
    pub value_attribute: String,
    pub ctl_model: Option<String>,
    pub sbo_timeout_ms: Option<i32>,
    pub interlock_check: bool,
    pub synchro_check: bool,
    pub deadband: Option<f64>,
    /// Tunggu CommandTermination (ms). None = env OPER_TIMEOUT_MS
    pub oper_timeout_ms: Option<i32>,
}

/// Nilai terakhir satu titik (gabungan value + q + t dari anggota-anggota dataset).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PointState {
    pub value: Value,
    pub q: Option<u16>,
    /// (ms epoch UTC, time quality)
    pub t: Option<(i64, u8)>,
}

