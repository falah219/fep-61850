//! Koneksi ke IED: manager, heartbeat, ping, reconnect (pola `connection/` FEP IEC 104).

mod gate;
mod health_check;
mod manager;
mod network_ping;
mod reconnection;

pub use gate::{ConnectGate, GatePermit};
#[cfg(test)]
pub(crate) use manager::testing;
pub use manager::{join_timeout, Debouncer, Manager};
pub use reconnection::run_ied;
