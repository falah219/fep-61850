//! Connection manager: satu thread per IED, start/stop/reload dari event Kafka,
//! heartbeat RTU_STATUS, dan ping jaringan (pola sama dengan FEP IEC 104).

use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use deadpool_postgres::Pool;
use futures::{stream, StreamExt};
use surge_ping::{Client, Config, PingIdentifier, PingSequence};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::config::RuntimeConfig;
use crate::iec61850::acquisition::{run_ied, ConnectGate, Ctl, RtuCtx};
use crate::iec61850::client::RcbSettings;
use crate::iec61850::mapping::Point;
use crate::kafka::models::safe_ip;
use crate::publisher::Publisher;
use crate::repository::{self as repo, RtuConfig};

struct Handle {
    ctx: Arc<RtuCtx>,
    tx: Sender<Ctl>,
}

struct PingStatus {
    last_status: i32,
    same_count: u32,
}

pub struct Manager {
    pub pool: Pool,
    pub publisher: Arc<Publisher>,
    pub runtime: RuntimeConfig,
    gate: Arc<ConnectGate>,
    rtus: DashMap<i32, Handle>,
    ping: DashMap<i32, PingStatus>,
}

impl Manager {
    pub fn new(pool: Pool, publisher: Arc<Publisher>, runtime: RuntimeConfig) -> Self {
        let gate = Arc::new(ConnectGate::new(runtime.max_concurrent_connects));
        Self { pool, publisher, runtime, gate, rtus: DashMap::new(), ping: DashMap::new() }
    }

    async fn client(&self) -> Result<deadpool_postgres::Client, String> {
        self.pool.get().await.map_err(|e| format!("gagal ambil koneksi DB: {e}"))
    }

    /// Muat semua IED aktif (state_rtu = true) lalu jalankan.
    pub async fn start_all(&self) -> Result<usize, String> {
        let c = self.client().await?;
        let rtus = repo::load_rtus(&c, None).await?;
        let rcbs = repo::load_rcbs(&c, None).await?;
        let points = repo::load_points(&c, None).await?;
        let mut started = 0;
        for rtu in rtus.into_iter().filter(|r| r.state_rtu) {
            let id = rtu.id_rtu;
            let rtu_rcbs = rcbs.iter().filter(|(r, _, _)| *r == id).map(|(_, n, s)| (n.clone(), s.clone())).collect();
            self.spawn(rtu, rtu_rcbs, points.rtu_points(id).cloned().collect())?;
            started += 1;
        }
        Ok(started)
    }

    /// Baca ulang satu RTU dari DB lalu restart threadnya.
    /// `state` dari event (STATE) menimpa nilai DB, untuk berjaga-jaga DB belum ter-update.
    pub async fn reload(&self, id: i32, state: Option<bool>) -> Result<(), String> {
        let c = self.client().await?;
        let Some(mut rtu) = repo::load_rtus(&c, Some(id)).await?.into_iter().next() else {
            self.stop(id);
            return Err(format!("RTU {id} tidak ada di iec61850s"));
        };
        if let Some(s) = state {
            rtu.state_rtu = s;
        }
        if !rtu.state_rtu {
            if self.stop(id) {
                self.publisher.producer.send_status(&rtu.ip_address, id, 0);
            }
            info!(id_rtu = id, "RTU nonaktif, tidak dikoneksikan");
            return Ok(());
        }
        let rcbs = repo::load_rcbs(&c, Some(id)).await?.into_iter().map(|(_, n, s)| (n, s)).collect();
        let points = repo::load_points(&c, Some(id)).await?.rtu_points(id).cloned().collect();
        self.spawn(rtu, rcbs, points)
    }

    /// Muat ulang semua RTU yang sedang berjalan (mis. mapping berubah tanpa tahu id_rtu-nya).
    pub async fn reload_all(&self) {
        let ids: Vec<i32> = self.rtus.iter().map(|e| *e.key()).collect();
        for id in ids {
            if let Err(e) = self.reload(id, None).await {
                warn!(id_rtu = id, "reload gagal: {e}");
            }
        }
    }

    fn spawn(&self, rtu: RtuConfig, rcbs: Vec<(String, RcbSettings)>, points: Vec<Point>) -> Result<(), String> {
        let id = rtu.id_rtu;
        self.stop(id);
        if points.is_empty() {
            warn!(id_rtu = id, "tidak ada titik termapping, IED tetap dikoneksikan");
        }
        let ctx = Arc::new(RtuCtx::new(rtu, rcbs, points, Arc::clone(&self.publisher), Arc::clone(&self.gate)));
        let (tx, rx) = channel();
        let thread_ctx = Arc::clone(&ctx);
        // Satu thread OS per IED: API libIEC61850 blocking, tidak boleh jalan di task async.
        std::thread::Builder::new()
            .name(format!("ied-{id}"))
            .spawn(move || run_ied(thread_ctx, rx))
            .map_err(|e| format!("gagal membuat thread IED {id}: {e}"))?;
        self.rtus.insert(id, Handle { ctx, tx });
        info!(id_rtu = id, "thread IED dijalankan");
        Ok(())
    }

    /// Hentikan thread IED. true jika memang sedang berjalan.
    pub fn stop(&self, id: i32) -> bool {
        match self.rtus.remove(&id) {
            Some((_, h)) => {
                let _ = h.tx.send(Ctl::Stop);
                true
            }
            None => false,
        }
    }

    pub fn stop_all(&self) {
        let ids: Vec<i32> = self.rtus.iter().map(|e| *e.key()).collect();
        for id in ids {
            self.stop(id);
        }
    }

    /// Kirim perintah ke thread IED. false jika IED tidak berjalan.
    pub fn send(&self, id: i32, ctl: Ctl) -> bool {
        self.rtus.get(&id).is_some_and(|h| h.tx.send(ctl).is_ok())
    }

    pub fn contains(&self, id: i32) -> bool {
        self.rtus.contains_key(&id)
    }

    pub fn ip_of(&self, id: i32) -> Option<String> {
        self.rtus.get(&id).map(|h| h.ctx.rtu.ip_address.clone())
    }

    pub fn id_by_ip(&self, ip: &str) -> Option<i32> {
        let ip = safe_ip(ip);
        self.rtus.iter().find(|e| e.ctx.rtu.ip_address == ip).map(|e| *e.key())
    }

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
