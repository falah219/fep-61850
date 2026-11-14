//! Akses Postgres (pola `repository/` FEP IEC 104).

mod pool;
mod resolver_repository;
mod rtu_repository;

pub use pool::create_pool;
pub use resolver_repository::{load_formulas, load_general_actions, load_groups, load_manual_sets, load_tags, rtu_of_datapoint};
pub use rtu_repository::{load_points, load_rcbs, load_rtus, DEFAULT_BRCB_RESV_TMS_SECS, DEFAULT_RCB_RETRY_SECS};

pub type DbResult<T> = Result<T, String>;

fn db_err(what: &str) -> impl Fn(tokio_postgres::Error) -> String + '_ {
    move |e| format!("{what}: {e}")
}
