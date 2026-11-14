//! Pool koneksi Postgres (padanan `repository/pool.rs` FEP IEC 104).

use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use super::DbResult;
use crate::config::DatabaseConfig;

/// Pool koneksi Postgres (sama seperti `repository/pool.rs` FEP IEC 104).
pub fn create_pool(db: &DatabaseConfig) -> DbResult<Pool> {
    let mut cfg = Config::new();
    cfg.host = Some(db.host.clone());
    cfg.port = Some(db.port);
    cfg.user = Some(db.user.clone());
    cfg.password = Some(db.password.clone());
    cfg.dbname = Some(db.dbname.clone());
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).map_err(|e| format!("gagal membuat pool DB: {e}"))
}
