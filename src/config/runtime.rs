//! Parameter runtime (koneksi, heartbeat, ping, reload, shutdown).

use std::env;

/// Parameter runtime dari env (nama & default sama dengan FEP IEC 104).
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub max_concurrent_connects: usize,
    pub status_heartbeat_interval_secs: u64,
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
    pub ping_attempts: u16,
    pub ping_retry_delay_ms: u64,
    pub ping_resend_every: u32,
    pub ping_concurrency: usize,
    /// Reload per id_rtu dari MASTER_DATAPOINT_EVENT / CDC ditunda & digabung selama ini.
    pub reload_debounce_ms: u64,
    /// Batas tunggu semua thread IED berhenti saat shutdown, sebelum flush Kafka.
    pub shutdown_wait_secs: u64,
    pub subscribe_topics: Vec<String>,
}

const DEFAULT_TOPICS: &str = "RTU_EVENTS,RTU_COMMAND_CONFIG_REQUEST,MASTER_DATAPOINT_EVENT,MASTER_DATA_OBJECT_EVENT,CAPTURE_DATA_CHANGE,MASTER_RTU_EVENTS,MANUAL_SET_EVENT";

/// Default kecil: banyak connect serentak di libIEC61850 bisa menghasilkan koneksi mati yang
/// getState()-nya tetap CONNECTED (lihat docs/CATATAN_TERBUKA.md).
pub const DEFAULT_MAX_CONCURRENT_CONNECTS: usize = 4;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl RuntimeConfig {
    pub fn from_env() -> Self {
        Self {
            max_concurrent_connects: env_or("MAX_CONCURRENT_CONNECTS", DEFAULT_MAX_CONCURRENT_CONNECTS).max(1),
            status_heartbeat_interval_secs: env_or("STATUS_HEARTBEAT_INTERVAL_SECS", 10).max(1),
            ping_interval_ms: env_or("PING_INTERVAL_MS", 5000).max(100),
            ping_timeout_ms: env_or("PING_TIMEOUT_MS", 2000),
            ping_attempts: env_or("PING_ATTEMPTS", 3).max(1),
            ping_retry_delay_ms: env_or("PING_RETRY_DELAY_MS", 200),
            ping_resend_every: env_or("PING_RESEND_EVERY", 6),
            ping_concurrency: env_or("PING_CONCURRENCY", 500).max(1),
            reload_debounce_ms: env_or("RELOAD_DEBOUNCE_MS", 2000),
            shutdown_wait_secs: env_or("SHUTDOWN_WAIT_SECS", 5),
            subscribe_topics: env::var("SUBSCRIBE_TOPICS")
                .unwrap_or_else(|_| DEFAULT_TOPICS.into())
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(String::from)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_connect_serentak() {
        assert_eq!(DEFAULT_MAX_CONCURRENT_CONNECTS, 4);
        assert_eq!(env_or::<usize>("FEP_TEST_ENV_TIDAK_ADA", DEFAULT_MAX_CONCURRENT_CONNECTS), 4);
    }
}
