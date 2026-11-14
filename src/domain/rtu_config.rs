//! Konfigurasi satu IED (baris `iec61850s` + parameter env), padanan `domain/rtu_config.rs` FEP IEC 104.

use crate::iec61850::client::OsiSide;

#[derive(Debug, Clone)]
pub struct RtuConfig {
    pub id_rtu: i32,
    pub state_rtu: bool,
    pub channel_number: i32,
    pub ip_address: String,
    pub port: i32,
    pub local_ip_address: String,
    pub acquisition_mode: String,
    pub poll_interval_ms: u64,
    pub general_interrogation_interval: u64,
    pub connect_timeout_ms: u32,
    pub request_timeout_ms: u32,
    pub reconnect_time_ms: u64,
    /// 0 = tanpa batas
    pub reconnect_retry_count: u32,
    /// Tidak ada di DB: kirim ulang semua nilai polling tiap periode ini (env INTEGRITY_PERIOD_MS).
    pub integrity_period_ms: u64,
    /// Tidak ada di DB: jeda coba ulang RCB yang gagal di-setup (env RCB_RETRY_SECS).
    pub rcb_retry_secs: u64,
    /// Tidak ada di DB: ResvTms BRCB (env BRCB_RESV_TMS_SECS)
    pub brcb_resv_tms_secs: i16,
    /// Tidak ada di DB: tanpa tanda hidup selama ini -> cek LLN0.Mod.stVal (env LIVENESS_SECS)
    pub liveness_secs: u64,
    /// Tidak ada di DB: default tunggu CommandTermination (env OPER_TIMEOUT_MS)
    pub oper_timeout_ms: u64,
    /// Tidak ada di DB: env OSI_PARAMS_SIDE
    pub osi_side: OsiSide,
    pub ap_title: String,
    pub ae_qualifier: i32,
    pub p_selector: String,
    pub s_selector: String,
    pub t_selector: String,
}

