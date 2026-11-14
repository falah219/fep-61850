//! Ping ICMP semua IED (padanan `connection/network_ping.rs` FEP IEC 104). Butuh CAP_NET_RAW / root.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use surge_ping::{Client, Config, PingIdentifier, PingSequence};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};

use super::Manager;
use crate::iec61850::acquisition::Ctl;

pub(super) struct PingStatus {
    last_status: i32,
    same_count: u32,
}

impl Manager {
    /// Ping ICMP semua IED (seperti `network_ping.rs` IEC 104). Butuh CAP_NET_RAW / root.
    pub async fn ping_loop(self: Arc<Self>) {
        let client = match Client::new(&Config::default()) {
            Ok(c) => c,
            Err(e) => return error!("gagal membuat client ICMP, ping dimatikan: {e}"),
        };
        let mut tick = interval(Duration::from_millis(self.runtime.ping_interval_ms));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let targets: Vec<(i32, String, i32)> = self
                .rtus
                .iter()
                .map(|e| (*e.key(), e.ctx.rtu.ip_address.clone(), e.ctx.rtu.channel_number))
                .collect();
            stream::iter(targets)
                .for_each_concurrent(self.runtime.ping_concurrency, |(id, ip, channel)| {
                    let client = &client;
                    let me = &self;
                    async move {
                        // Hostname (mis. ied-sim-1) tidak di-ping, sama seperti IEC 104.
                        let Ok(addr) = ip.parse::<IpAddr>() else { return };
                        let reachable = me.ping_host(client, addr).await;
                        me.update_ping(id, &ip, channel, reachable);
                    }
                })
                .await;
        }
    }

    async fn ping_host(&self, client: &Client, addr: IpAddr) -> bool {
        let rt = &self.runtime;
        let mut pinger = client.pinger(addr, PingIdentifier(random_u16())).await;
        pinger.timeout(Duration::from_millis(rt.ping_timeout_ms));
        for seq in 0..rt.ping_attempts {
            if pinger.ping(PingSequence(seq), &[]).await.is_ok() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(rt.ping_retry_delay_ms)).await;
        }
        false
    }

    fn update_ping(&self, id: i32, ip: &str, channel: i32, reachable: bool) {
        let status = reachable as i32;
        let (changed, send) = {
            let mut e = self.ping.entry(id).or_insert(PingStatus { last_status: -1, same_count: 0 });
            let changed = e.last_status != status;
            e.same_count = if changed { 0 } else { e.same_count + 1 };
            let resend = e.same_count >= self.runtime.ping_resend_every;
            if resend {
                e.same_count = 0;
            }
            e.last_status = status;
            (changed, changed || resend)
        };
        if changed && reachable {
            info!(id_rtu = id, endpoint = ip, "ied reachable");
            // Potong jeda reconnect supaya langsung konek.
            self.send(id, Ctl::Wake);
        } else if changed {
            warn!(id_rtu = id, endpoint = ip, "ied unreachable");
        }
        if send {
            self.publisher.producer.send_status_with_channel(ip, id, channel, status);
        }
    }
}

fn random_u16() -> u16 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    ((now.as_secs() ^ u64::from(now.subsec_nanos())) & 0xFFFF) as u16
}
