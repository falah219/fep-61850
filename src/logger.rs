//! Logger: console + file harian `logs/iec61850.YYYY-MM-DD` (format sama dengan `logger.rs` FEP IEC 104).

use tracing::warn;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::{self, time::ChronoLocal};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

const LOG_DIR: &str = "logs";

/// Pasang logger global. Simpan guard yang dikembalikan sampai proses selesai: saat di-drop,
/// sisa log di antrean ditulis ke file. Kalau folder `logs/` tidak bisa dibuat/ditulis
/// (mis. tidak ada izin), log hanya ke console dan FEP tetap jalan.
pub fn init_logger() -> Option<WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let console_layer = fmt::layer()
        .with_timer(ChronoLocal::new("%H:%M:%S%.3f".to_string()))
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .compact();

    let appender = std::fs::create_dir_all(LOG_DIR).map_err(|e| e.to_string()).and_then(|_| {
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_prefix("iec61850")
            .build(LOG_DIR)
            .map_err(|e| e.to_string())
    });
    let (file_layer, guard, file_err) = match appender {
        Ok(appender) => {
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
        Err(e) => (None, None, Some(e)),
    };

    tracing_subscriber::registry().with(env_filter).with(file_layer).with(console_layer).init();
    if let Some(e) = file_err {
        warn!("log file di {LOG_DIR}/ tidak bisa dibuat ({e}), log hanya ke console");
    }
    guard
}
