//! Context rdkafka: log & error librdkafka diteruskan ke `tracing` (target `librdkafka`), jadi
//! ikut tercatat di console dan file log (`logger.rs`).

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;
use rdkafka::producer::{DeliveryResult, ProducerContext};
use tracing::{debug, error, info, warn, Level};

/// Dipakai producer dan consumer.
#[derive(Clone, Copy, Default)]
pub struct KafkaLogContext;

/// Level syslog librdkafka -> level tracing.
pub fn tracing_level(level: RDKafkaLogLevel) -> Level {
    match level {
        RDKafkaLogLevel::Emerg | RDKafkaLogLevel::Alert | RDKafkaLogLevel::Critical | RDKafkaLogLevel::Error => Level::ERROR,
        RDKafkaLogLevel::Warning => Level::WARN,
        RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => Level::INFO,
        RDKafkaLogLevel::Debug => Level::DEBUG,
    }
}

impl ClientContext for KafkaLogContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        // Macro tracing butuh level konstan -> pilih macro per level.
        let lvl = tracing_level(level);
        if lvl == Level::ERROR {
            error!(target: "librdkafka", "{fac}: {log_message}");
        } else if lvl == Level::WARN {
            warn!(target: "librdkafka", "{fac}: {log_message}");
        } else if lvl == Level::INFO {
            info!(target: "librdkafka", "{fac}: {log_message}");
        } else {
            debug!(target: "librdkafka", "{fac}: {log_message}");
        }
    }

    /// Error global librdkafka (mis. semua broker tidak terjangkau, autentikasi SASL gagal).
    fn error(&self, error: KafkaError, reason: &str) {
        error!(target: "librdkafka", "{error}: {reason}");
    }
}

impl ConsumerContext for KafkaLogContext {}

impl ProducerContext for KafkaLogContext {
    type DeliveryOpaque = ();

    // ponytail: gagal kirim per pesan tidak di-log (bisa ribuan saat broker mati; error global
    // sudah masuk lewat `error()`). Tambah log di sini kalau perlu jejak per pesan.
    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_librdkafka_ke_tracing() {
        assert_eq!(tracing_level(RDKafkaLogLevel::Emerg), Level::ERROR);
        assert_eq!(tracing_level(RDKafkaLogLevel::Critical), Level::ERROR);
        assert_eq!(tracing_level(RDKafkaLogLevel::Error), Level::ERROR);
        assert_eq!(tracing_level(RDKafkaLogLevel::Warning), Level::WARN);
        assert_eq!(tracing_level(RDKafkaLogLevel::Notice), Level::INFO);
        assert_eq!(tracing_level(RDKafkaLogLevel::Info), Level::INFO);
        assert_eq!(tracing_level(RDKafkaLogLevel::Debug), Level::DEBUG);
    }
}
