//! Logger: console + file harian `logs/iec61850.YYYY-MM-DD` (format sama dengan `logger.rs` FEP IEC 104).
//!
//! Env:
//! - `LOG_TO_FILE` (default true): tulis juga ke file. Matikan di Kubernetes (log dibaca dari stdout).
//! - `LOG_MAX_FILES` (default 14): jumlah file harian yang disimpan, yang lebih lama dihapus. 0 = tanpa batas.

use std::io::IsTerminal;

use tracing::warn;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::{self, time::ChronoLocal};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

const LOG_DIR: &str = "logs";
const DEFAULT_MAX_FILES: usize = 14;

/// Nilai env boolean: `false`/`0`/`no`/`off` = false, `true`/`1`/`yes`/`on` = true, lainnya = default.
pub fn env_flag(value: Option<&str>, default: bool) -> bool {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("false" | "0" | "no" | "off") => false,
        Some("true" | "1" | "yes" | "on") => true,
        _ => default,
    }
}

/// `LOG_MAX_FILES`: None = tanpa batas (nilai 0).
pub fn max_files(value: Option<&str>) -> Option<usize> {
    let n = value.and_then(|v| v.trim().parse().ok()).unwrap_or(DEFAULT_MAX_FILES);
    (n > 0).then_some(n)
}

fn file_appender(max: Option<usize>) -> Result<RollingFileAppender, String> {
    std::fs::create_dir_all(LOG_DIR).map_err(|e| e.to_string())?;
    let mut builder = RollingFileAppender::builder().rotation(Rotation::DAILY).filename_prefix("iec61850");
    if let Some(n) = max {
        builder = builder.max_log_files(n);
    }
    builder.build(LOG_DIR).map_err(|e| e.to_string())
}

/// Pasang logger global. Simpan guard yang dikembalikan sampai proses selesai: saat di-drop,
/// sisa log di antrean ditulis ke file. Kalau folder `logs/` tidak bisa dibuat/ditulis
/// (mis. tidak ada izin), log hanya ke console dan FEP tetap jalan.
pub fn init_logger() -> Option<WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let to_file = env_flag(std::env::var("LOG_TO_FILE").ok().as_deref(), true);
    let max = max_files(std::env::var("LOG_MAX_FILES").ok().as_deref());

    let console_layer = fmt::layer()
        // Warna hanya kalau stdout terminal; di docker logs / kubectl logs / file tetap bersih.
        .with_ansi(std::io::stdout().is_terminal())
        .with_timer(ChronoLocal::new("%H:%M:%S%.3f".to_string()))
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .compact();

    let appender = if to_file { Some(file_appender(max)) } else { None };
    let (file_layer, guard, file_err) = match appender {
        Some(Ok(appender)) => {
            let (writer, guard) = tracing_appender::non_blocking(appender);
            let layer = fmt::layer()
                .with_writer(writer)
                .with_ansi(false) // tanpa warna di file
                .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f".to_string()))
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true);
            (Some(layer), Some(guard), None)
        }
        Some(Err(e)) => (None, None, Some(e)),
        None => (None, None, None),
    };

    tracing_subscriber::registry().with(env_filter).with(file_layer).with(console_layer).init();
    if let Some(e) = file_err {
        warn!("log file di {LOG_DIR}/ tidak bisa dibuat ({e}), log hanya ke console");
    }
    guard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_env() {
        assert!(env_flag(None, true));
        assert!(!env_flag(Some("false"), true));
        assert!(!env_flag(Some(" OFF "), true));
        assert!(!env_flag(Some("0"), true));
        assert!(env_flag(Some("yes"), false));
        assert!(env_flag(Some("ngawur"), true)); // tidak dikenal -> default
    }

    #[test]
    fn jumlah_file_log() {
        assert_eq!(max_files(None), Some(14));
        assert_eq!(max_files(Some("30")), Some(30));
        assert_eq!(max_files(Some("0")), None); // tanpa batas
        assert_eq!(max_files(Some("abc")), Some(14));
    }
}
