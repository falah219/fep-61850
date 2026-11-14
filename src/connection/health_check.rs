//! Heartbeat RTU_STATUS semua IED (padanan `connection/health_check.rs` FEP IEC 104).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tracing::info;

use super::Manager;

impl Manager {
    /// Heartbeat RTU_STATUS tiap STATUS_HEARTBEAT_INTERVAL_SECS (seperti `health_check.rs` IEC 104).
    pub async fn heartbeat_loop(self: Arc<Self>) {
        let mut tick = interval(Duration::from_secs(self.runtime.status_heartbeat_interval_secs));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let (mut up, mut down) = (0, 0);
            for e in self.rtus.iter() {
                let connected = e.ctx.connected.load(Ordering::SeqCst);
                if connected { up += 1 } else { down += 1 }
                self.publisher.producer.send_status(&e.ctx.rtu.ip_address, *e.key(), connected as i32);
            }
            info!(total = up + down, connection_up = up, connection_down = down, "ied health check");
        }
    }
}
