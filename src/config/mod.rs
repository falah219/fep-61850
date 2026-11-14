//! Konfigurasi dari env (pola folder sama dengan `src/config/` FEP IEC 104). Nama variabel env
//! sama dengan FEP IEC 104.

mod database;
mod kafka;
mod runtime;

pub use database::DatabaseConfig;
pub use kafka::KafkaConfig;
pub use runtime::{RuntimeConfig, DEFAULT_MAX_CONCURRENT_CONNECTS};
