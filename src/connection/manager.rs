//! Connection manager: satu thread per IED, start/stop/reload dari event Kafka
//! (padanan `connection/manager.rs` FEP IEC 104). Heartbeat di `health_check.rs`, ping di `network_ping.rs`.

use crate::iec61850::traffic::TrafficLog;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use deadpool_postgres::Pool;
use tracing::{info, warn};

use crate::config::RuntimeConfig;
use crate::error::{Iec61850Error::Database, Result};
use super::network_ping::PingStatus;
use super::{run_ied, ConnectGate};
use crate::domain::{Point, RtuConfig};
use crate::iec61850::acquisition::{Ctl, RtuCtx};
use crate::iec61850::client::RcbSettings;
use crate::kafka::publisher::Publisher;
use crate::repository as repo;

/// Batas tunggu thread IED lama berhenti. Thread bisa sedang di panggilan MMS blocking
/// (connect / read / operate), yang masing-masing dibatasi timeout-nya sendiri.
// ponytail: angka tetap; jadikan per-RTU (connect_timeout_ms + request_timeout_ms) kalau ada IED lambat
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

/// Kunci debounce untuk `reload_all` (id_rtu tidak diketahui).
const ALL_RTUS: i32 = i32::MIN;

pub(super) struct Handle {
    pub(super) ctx: Arc<RtuCtx>,
    tx: Sender<Ctl>,
    join: JoinHandle<()>,
}

impl Handle {
    fn signal_stop(&self) {
        self.ctx.stopping.store(true, Ordering::SeqCst);
        let _ = self.tx.send(Ctl::Stop);
    }

    async fn wait(self, limit: Duration) {
        let id = self.ctx.rtu.id_rtu;
        if !join_timeout(self.join, limit).await {
            warn!(id_rtu = id, "thread IED belum berhenti setelah {limit:?}, ditinggal");
        }
    }
}

/// Tunggu thread selesai maks `limit` tanpa memblok runtime tokio. false = timeout.
pub async fn join_timeout(h: JoinHandle<()>, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while !h.is_finished() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = h.join();
    true
}

/// Gabungkan pekerjaan yang dijadwalkan berulang untuk kunci yang sama: hanya jadwal terakhir
/// yang jalan, `delay` setelah jadwal itu dibuat.
#[derive(Default)]
pub struct Debouncer {
    generation: Arc<DashMap<i32, u64>>,
}

impl Debouncer {
    pub fn schedule<F>(&self, key: i32, delay: Duration, job: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mine = {
            let mut g = self.generation.entry(key).or_insert(0);
            *g += 1;
            *g
        };
        let generation = Arc::clone(&self.generation);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if generation.remove_if(&key, |_, g| *g == mine).is_some() {
                job.await;
            }
        });
    }
}

pub struct Manager {
    pub pool: Pool,
    pub publisher: Arc<Publisher>,
    pub runtime: RuntimeConfig,
    gate: Arc<ConnectGate>,
    pub(super) rtus: DashMap<i32, Handle>,
    pub(super) ping: DashMap<i32, PingStatus>,
    /// Serialisasi stop/reload: thread lama selalu berhenti dulu sebelum thread baru dibuat,
    /// jadi tidak pernah ada dua sesi MMS ke IED yang sama (RCB rebutan).
    ops: tokio::sync::Mutex<()>,
    /// true setelah stop_all (shutdown): reload yang masih tertunda tidak dijalankan.
    closed: AtomicBool,
    debounce: Debouncer,
    /// Salinan TERAKHIR baris `iec61850s` dari DB per IED (apa adanya, tanpa override event STATE),
    /// terpisah dari status jalan/berhenti. Pembanding CDC `iec61850s`.
    db_rows: DashMap<i32, RtuConfig>,
    /// Debounce khusus cek-ulang baris CDC `iec61850s` (kunci terpisah dari `debounce`, supaya tidak
    /// menimpa reload penuh yang sedang tertunda untuk IED yang sama).
    row_debounce: Debouncer,
}

/// IP / hostname dibandingkan apa adanya: trim + huruf kecil, TIDAK ada karakter yang dibuang
/// (hostname seperti `ied-sim-1` harus cocok).
pub fn same_host(a: &str, b: &str) -> bool {
    a.trim().to_lowercase() == b.trim().to_lowercase()
}

/// Tindakan setelah baris `iec61850s` dibaca ulang dari DB (dibandingkan dengan salinan terakhir).
#[derive(Debug, PartialEq)]
pub enum RowAction {
    /// Sama dengan salinan: tidak ada yang dilakukan (mis. sudah ditangani event STATE)
    Skip,
    Reload,
    /// Baris hilang / is_deleted
    Stop,
}

pub fn row_action(old: Option<&RtuConfig>, new: Option<&RtuConfig>) -> RowAction {
    match (old, new) {
        (_, None) => RowAction::Stop,
        (Some(o), Some(n)) if o == n => RowAction::Skip,
        _ => RowAction::Reload,
    }
}

impl Manager {
    pub fn new(pool: Pool, publisher: Arc<Publisher>, runtime: RuntimeConfig) -> Self {
        let gate = Arc::new(ConnectGate::new(runtime.max_concurrent_connects));
        Self {
            pool,
            publisher,
            runtime,
            gate,
            rtus: DashMap::new(),
            ping: DashMap::new(),
            ops: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
            debounce: Debouncer::default(),
            db_rows: DashMap::new(),
            row_debounce: Debouncer::default(),
        }
    }

    async fn client(&self) -> Result<deadpool_postgres::Client> {
        self.pool.get().await.map_err(|e| Database(format!("gagal ambil koneksi DB: {e}")))
    }

    /// Muat semua IED aktif (state_rtu = true) lalu jalankan.
    pub async fn start_all(&self) -> Result<usize> {
        let _ops = self.ops.lock().await;
        let c = self.client().await?;
        let rtus = repo::load_rtus(&c, None).await.map_err(Database)?;
        let rcbs = repo::load_rcbs(&c, None).await.map_err(Database)?;
        let points = repo::load_points(&c, None).await.map_err(Database)?;
        let unmapped = repo::load_unmapped_ioas(&c, None).await.map_err(Database)?;
        let mut started = 0;
        for rtu in &rtus {
            self.db_rows.insert(rtu.id_rtu, rtu.clone());
        }
        for rtu in rtus.into_iter().filter(|r| r.state_rtu) {
            let id = rtu.id_rtu;
            let rtu_rcbs = rcbs.iter().filter(|(r, _, _)| *r == id).map(|(_, n, s)| (n.clone(), s.clone())).collect();
            let rtu_unmapped = unmapped.iter().filter(|(r, _)| *r == id).map(|(_, ioa)| *ioa).collect();
            self.spawn(rtu, rtu_rcbs, points.rtu_points(id).cloned().collect(), rtu_unmapped).await?;
            started += 1;
        }
        Ok(started)
    }

    /// Baca ulang satu RTU dari DB lalu restart threadnya.
    /// `state` dari event (STATE) menimpa nilai DB, untuk berjaga-jaga DB belum ter-update.
    pub async fn reload(&self, id: i32, state: Option<bool>) -> Result<()> {
        let _ops = self.ops.lock().await;
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let c = self.client().await?;
        let Some(mut rtu) = repo::load_rtus(&c, Some(id)).await.map_err(Database)?.into_iter().next() else {
            self.db_rows.remove(&id);
            self.stop_locked(id).await;
            return Err(format!("RTU {id} tidak ada di iec61850s").into());
        };
        // salinan DB apa adanya (sebelum override STATE)
        self.db_rows.insert(id, rtu.clone());
        if let Some(s) = state {
            rtu.state_rtu = s;
        }
        if !rtu.state_rtu {
            if self.stop_locked(id).await {
                self.publisher.producer.send_status(&rtu.ip_address, id, 0);
            }
            info!(id_rtu = id, "RTU nonaktif, tidak dikoneksikan");
            return Ok(());
        }
        let rcbs = repo::load_rcbs(&c, Some(id)).await.map_err(Database)?.into_iter().map(|(_, n, s)| (n, s)).collect();
        let points = repo::load_points(&c, Some(id)).await.map_err(Database)?.rtu_points(id).cloned().collect();
        let unmapped = repo::load_unmapped_ioas(&c, Some(id)).await.map_err(Database)?.into_iter().map(|(_, ioa)| ioa).collect();
        self.spawn(rtu, rcbs, points, unmapped).await
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

    /// Event STATE (idempoten): IED yang sudah jalan tidak di-restart oleh STATE true, IED yang sudah berhenti
    /// tidak diapa-apakan oleh STATE false. Jadi STATE + CDC `state_rtu` untuk perubahan yang sama = satu aksi.
    pub async fn set_state(&self, id: i32, state: bool) -> Result<()> {
        if state == self.contains(id) {
            info!(id_rtu = id, state, "STATE sama dengan kondisi IED, tidak ada aksi");
            return Ok(());
        }
        self.reload(id, Some(state)).await
    }

    /// Salinan terakhir baris DB IED ini.
    pub fn db_row(&self, id: i32) -> Option<RtuConfig> {
        self.db_rows.get(&id).map(|r| r.clone())
    }

    /// `channel_number` berubah: cukup perbarui channel status ping, TANPA reconnect MMS.
    pub fn set_channel(&self, id: i32, channel: i32) {
        if let Some(mut row) = self.db_row(id).or_else(|| self.config_of(id)) {
            row.channel_number = channel;
            self.db_rows.insert(id, row);
        }
    }

    /// Channel untuk status ping: salinan DB terbaru, kalau belum ada konfigurasi thread.
    pub(super) fn channel_of(&self, id: i32, running: i32) -> i32 {
        self.db_rows.get(&id).map_or(running, |r| r.channel_number)
    }

    /// CDC `iec61850s`: setelah RELOAD_DEBOUNCE_MS baca ulang baris dari DB dan bandingkan dengan salinan
    /// terakhir. Sama -> tidak ada aksi (mis. event STATE sudah memprosesnya lebih dulu).
    pub fn reload_row_later(self: &Arc<Self>, id: i32) {
        let me = Arc::clone(self);
        let delay = Duration::from_millis(self.runtime.reload_debounce_ms);
        self.row_debounce.schedule(id, delay, async move {
            if let Err(e) = me.reload_if_row_changed(id).await {
                warn!(id_rtu = id, "reload gagal: {e}");
            }
        });
    }

    async fn reload_if_row_changed(&self, id: i32) -> Result<()> {
        let row = {
            let c = self.client().await?;
            repo::load_rtus(&c, Some(id)).await.map_err(Database)?.into_iter().next()
        };
        match row_action(self.db_row(id).as_ref(), row.as_ref()) {
            RowAction::Skip => {
                info!(id_rtu = id, "baris iec61850s sama dengan salinan terakhir, tidak ada aksi");
                Ok(())
            }
            RowAction::Stop => {
                self.db_rows.remove(&id);
                self.stop(id).await;
                Ok(())
            }
            RowAction::Reload => self.reload(id, None).await,
        }
    }

    /// Reload tertunda RELOAD_DEBOUNCE_MS: rentetan event untuk RTU yang sama (mis. import
    /// ratusan datapoint) jadi SATU reload. `id = None` -> reload_all.
    pub fn reload_later(self: &Arc<Self>, id: Option<i32>, state: Option<bool>) {
        let me = Arc::clone(self);
        let delay = Duration::from_millis(self.runtime.reload_debounce_ms);
        self.debounce.schedule(id.unwrap_or(ALL_RTUS), delay, async move {
            match id {
                Some(id) => {
                    if let Err(e) = me.reload(id, state).await {
                        warn!(id_rtu = id, "reload gagal: {e}");
                    }
                }
                None => me.reload_all().await,
            }
        });
    }

    async fn spawn(&self, rtu: RtuConfig, rcbs: Vec<(String, RcbSettings)>, points: Vec<Point>, unmapped_ioas: Vec<i32>) -> Result<()> {
        let id = rtu.id_rtu;
        // Tunggu thread lama benar-benar selesai (koneksi MMS ditutup) sebelum konek lagi.
        self.stop_locked(id).await;
        if points.is_empty() {
            warn!(id_rtu = id, "tidak ada titik termapping, IED tetap dikoneksikan");
        }
        if matches!(rtu.acquisition_mode.as_str(), "REPORT" | "HYBRID") && rcbs.is_empty() {
            warn!(id_rtu = id, mode = %rtu.acquisition_mode, "tidak ada RCB enabled di iec61850_report_controls, report tidak akan masuk");
        }
        let mut ctx = RtuCtx::new(rtu, rcbs, points, Arc::clone(&self.publisher), Arc::clone(&self.gate));
        ctx.unmapped_ioas = unmapped_ioas;
        ctx.traffic = TrafficLog::new(self.runtime.log_data_traffic, self.runtime.log_data_max_per_sec);
        let ctx = Arc::new(ctx);
        let thread_ctx = Arc::clone(&ctx);
        // Satu thread OS per IED: API libIEC61850 blocking, tidak boleh jalan di task async.
        self.start_thread(ctx, move |rx| run_ied(thread_ctx, rx))?;
        info!(id_rtu = id, "thread IED dijalankan");
        Ok(())
    }

    fn start_thread(&self, ctx: Arc<RtuCtx>, run: impl FnOnce(Receiver<Ctl>) + Send + 'static) -> Result<()> {
        let id = ctx.rtu.id_rtu;
        let (tx, rx) = channel();
        let join = std::thread::Builder::new()
            .name(format!("ied-{id}"))
            .spawn(move || run(rx))
            .map_err(|e| format!("gagal membuat thread IED {id}: {e}"))?;
        self.rtus.insert(id, Handle { ctx, tx, join });
        Ok(())
    }

    /// Hentikan thread IED dan tunggu selesai (maks STOP_TIMEOUT). true jika memang sedang berjalan.
    pub async fn stop(&self, id: i32) -> bool {
        let _ops = self.ops.lock().await;
        self.stop_locked(id).await
    }

    async fn stop_locked(&self, id: i32) -> bool {
        let Some((_, h)) = self.rtus.remove(&id) else { return false };
        h.signal_stop();
        h.wait(STOP_TIMEOUT).await;
        true
    }

    /// Shutdown: hentikan semua thread serentak dan tunggu maks `limit` (SHUTDOWN_WAIT_SECS), lalu
    /// kirim RTU_STATUS=0 untuk SEMUA IED (termasuk yang threadnya macet / belum pernah konek),
    /// sebelum producer di-flush. Status 0 dobel dari thread sendiri tidak masalah (sama seperti
    /// heartbeat). Kembalikan id_rtu yang dikirimi status 0.
    pub async fn stop_all(&self, limit: Duration) -> Vec<i32> {
        let _ops = self.ops.lock().await;
        self.closed.store(true, Ordering::SeqCst);
        let ids: Vec<i32> = self.rtus.iter().map(|e| *e.key()).collect();
        let handles: Vec<Handle> = ids.into_iter().filter_map(|id| self.rtus.remove(&id).map(|(_, h)| h)).collect();
        let targets: Vec<(i32, String)> = handles.iter().map(|h| (h.ctx.rtu.id_rtu, h.ctx.rtu.ip_address.clone())).collect();
        for h in &handles {
            h.signal_stop();
        }
        futures::future::join_all(handles.into_iter().map(|h| h.wait(limit))).await;
        for (id, ip) in &targets {
            self.publisher.producer.send_status(ip, *id, 0);
        }
        targets.into_iter().map(|(id, _)| id).collect()
    }

    /// Kirim perintah ke thread IED. false jika IED tidak berjalan.
    pub fn send(&self, id: i32, ctl: Ctl) -> bool {
        self.rtus.get(&id).is_some_and(|h| h.tx.send(ctl).is_ok())
    }

    pub fn contains(&self, id: i32) -> bool {
        self.rtus.contains_key(&id)
    }

    /// (ip_address, port) IED dari konfigurasi yang sedang berjalan.
    pub fn endpoint_of(&self, id: i32) -> Option<(String, i32)> {
        self.rtus.get(&id).map(|h| (h.ctx.rtu.ip_address.clone(), h.ctx.rtu.port))
    }

    /// Konfigurasi IED yang sedang berjalan (untuk membandingkan CDC `iec61850s`).
    pub fn config_of(&self, id: i32) -> Option<RtuConfig> {
        self.rtus.get(&id).map(|h| h.ctx.rtu.clone())
    }

    pub fn ip_of(&self, id: i32) -> Option<String> {
        self.rtus.get(&id).map(|h| h.ctx.rtu.ip_address.clone())
    }

    /// IED yang sedang berjalan dengan `ip_address` ini (COMMAND, BROWSE, GI, STATE, ... tanpa id_rtu).
    pub fn id_by_ip(&self, ip: &str) -> Option<i32> {
        if ip.trim().is_empty() {
            return None;
        }
        self.rtus.iter().find(|e| same_host(&e.ctx.rtu.ip_address, ip)).map(|e| *e.key())
    }

}

/// Manager tanpa DB & broker sungguhan (pool lazy, producer ke port tertutup) untuk test.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::config::{DatabaseConfig, KafkaConfig};
    use crate::kafka::KafkaProducer;
    use crate::resolver::{FormulaResolver, GeneralActionResolver, GroupResolver, TagResolver};

    pub fn manager() -> Arc<Manager> {
        let kafka = KafkaConfig {
            bootstrap_servers: "127.0.0.1:1".into(),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: String::new(),
            sasl_username: String::new(),
            sasl_password: String::new(),
            client_id: "test".into(),
            group_id: "test".into(),
            auto_offset_reset: "latest".into(),
        };
        let db = DatabaseConfig { host: "127.0.0.1".into(), port: 1, user: "x".into(), password: String::new(), dbname: "x".into() };
        let publisher = Arc::new(Publisher {
            producer: KafkaProducer::new(&kafka).expect("producer test"),
            tags: TagResolver::new(),
            formulas: FormulaResolver::new(),
            groups: GroupResolver::new(),
            general_actions: GeneralActionResolver::new(),
        });
        let pool = repo::create_pool(&db).expect("pool test");
        Arc::new(Manager::new(pool, publisher, RuntimeConfig::from_env()))
    }

    pub fn rtu(id: i32) -> RtuConfig {
        RtuConfig {
            id_rtu: id,
            state_rtu: true,
            channel_number: 1,
            ip_address: format!("ied-{id}"),
            port: 102,
            local_ip_address: "0.0.0.0".into(),
            acquisition_mode: "REPORT".into(),
            poll_interval_ms: 1000,
            general_interrogation_interval: 0,
            connect_timeout_ms: 1000,
            request_timeout_ms: 1000,
            reconnect_time_ms: 1000,
            reconnect_retry_count: 0,
            integrity_period_ms: 60_000,
            rcb_retry_secs: 30,
            brcb_resv_tms_secs: 20,
            liveness_secs: 30,
            oper_timeout_ms: 10_000,
            dpc_close_value: 2,
            osi_side: crate::iec61850::client::OsiSide::None,
            ap_title: "1.1.999.1".into(),
            ae_qualifier: 12,
            p_selector: "00 00 00 01".into(),
            s_selector: "00 01".into(),
            t_selector: "00 01".into(),
        }
    }

    /// Daftarkan "thread IED" palsu yang menjalankan `run`.
    pub fn fake_ied(mgr: &Manager, id: i32, run: impl FnOnce(Receiver<Ctl>) + Send + 'static) {
        fake_ied_cfg(mgr, rtu(id), run);
    }

    /// Sama dengan `fake_ied`, konfigurasi sendiri (mis. IP literal untuk `id_by_ip`).
    pub fn fake_ied_cfg(mgr: &Manager, cfg: RtuConfig, run: impl FnOnce(Receiver<Ctl>) + Send + 'static) {
        let ctx = Arc::new(RtuCtx::new(cfg, vec![], vec![], Arc::clone(&mgr.publisher), Arc::clone(&mgr.gate)));
        mgr.start_thread(ctx, run).expect("thread test");
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Thread IED palsu: setelah Stop masih butuh `cleanup` (mis. menutup koneksi MMS).
    fn slow_stopper(done: Arc<AtomicUsize>, cleanup: Duration) -> impl FnOnce(Receiver<Ctl>) + Send + 'static {
        move |rx| {
            while !matches!(rx.recv(), Ok(Ctl::Stop) | Err(_)) {}
            std::thread::sleep(cleanup);
            done.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn join_dengan_batas_waktu() {
        let quick = std::thread::spawn(|| std::thread::sleep(Duration::from_millis(50)));
        assert!(join_timeout(quick, Duration::from_secs(2)).await);
        let stuck = std::thread::spawn(|| std::thread::sleep(Duration::from_secs(2)));
        let t = Instant::now();
        assert!(!join_timeout(stuck, Duration::from_millis(100)).await);
        assert!(t.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn cari_ied_dari_ip_apa_adanya() {
        let mgr = manager();
        let idle = |rx: Receiver<Ctl>| while !matches!(rx.recv(), Ok(Ctl::Stop) | Err(_)) {};
        for (id, ip) in [(1, "ied-sim-1"), (2, "192.168.212.25"), (3, "IED-Sim-2")] {
            fake_ied_cfg(&mgr, RtuConfig { ip_address: ip.into(), ..rtu(id) }, idle);
        }
        assert_eq!(mgr.id_by_ip("ied-sim-1"), Some(1));
        assert_eq!(mgr.id_by_ip(" IED-SIM-1 "), Some(1));
        assert_eq!(mgr.id_by_ip("192.168.212.25"), Some(2));
        assert_eq!(mgr.id_by_ip("ied-sim-2"), Some(3));
        assert_eq!(mgr.id_by_ip("iedsim1"), None); // tidak ada karakter yang dibuang
        assert_eq!(mgr.id_by_ip("192.168.212.2"), None);
        assert_eq!(mgr.id_by_ip(""), None);
        assert!(same_host("Ied-Sim-1", "ied-sim-1 "));
        mgr.stop_all(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn stop_menunggu_thread_lama_selesai() {
        let mgr = manager();
        let done = Arc::new(AtomicUsize::new(0));
        fake_ied(&mgr, 1, slow_stopper(Arc::clone(&done), Duration::from_millis(300)));
        assert!(mgr.stop(1).await);
        // stop() baru kembali setelah thread lama selesai -> aman spawn thread baru
        assert_eq!(done.load(Ordering::SeqCst), 1);
        assert!(!mgr.contains(1));
        assert!(!mgr.stop(1).await);
    }

    #[tokio::test]
    async fn stop_all_menunggu_semua_thread() {
        let mgr = manager();
        let done = Arc::new(AtomicUsize::new(0));
        for id in 1..=3 {
            fake_ied(&mgr, id, slow_stopper(Arc::clone(&done), Duration::from_millis(300)));
        }
        let t = Instant::now();
        let mut stopped = mgr.stop_all(Duration::from_secs(5)).await;
        assert_eq!(done.load(Ordering::SeqCst), 3);
        // serentak, bukan 3 x 300 ms
        assert!(t.elapsed() < Duration::from_millis(800));
        stopped.sort();
        assert_eq!(stopped, vec![1, 2, 3]); // RTU_STATUS=0 untuk semua IED
        // setelah shutdown, reload tertunda tidak menjalankan apa-apa (tanpa menyentuh DB)
        assert_eq!(mgr.reload(1, None).await, Ok(()));
        assert!(!mgr.contains(1));
    }

    #[tokio::test]
    async fn shutdown_tidak_menunggu_lebih_dari_batas() {
        let mgr = manager();
        let done = Arc::new(AtomicUsize::new(0));
        fake_ied(&mgr, 1, slow_stopper(Arc::clone(&done), Duration::from_millis(50)));
        // thread macet (mis. panggilan MMS blocking) yang butuh 3 detik untuk berhenti
        fake_ied(&mgr, 2, slow_stopper(Arc::clone(&done), Duration::from_secs(3)));
        let t = Instant::now();
        let mut stopped = mgr.stop_all(Duration::from_millis(300)).await;
        assert!(t.elapsed() < Duration::from_secs(1));
        assert_eq!(done.load(Ordering::SeqCst), 1);
        stopped.sort();
        // IED yang macet tetap dikirimi RTU_STATUS=0
        assert_eq!(stopped, vec![1, 2]);
    }

    #[tokio::test]
    async fn debounce_per_kunci() {
        let d = Debouncer::default();
        let runs = Arc::new(DashMap::<i32, usize>::new());
        for key in [1, 1, 1, 2, 1] {
            let runs = Arc::clone(&runs);
            d.schedule(key, Duration::from_millis(100), async move {
                *runs.entry(key).or_insert(0) += 1;
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        // 4 event untuk RTU 1 -> 1 reload; RTU 2 tetap jalan sendiri
        assert_eq!(runs.get(&1).map(|v| *v), Some(1));
        assert_eq!(runs.get(&2).map(|v| *v), Some(1));
        // jadwal baru setelah selesai jalan lagi
        let r = Arc::clone(&runs);
        d.schedule(1, Duration::from_millis(10), async move { *r.entry(1).or_insert(0) += 1 });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(runs.get(&1).map(|v| *v), Some(2));
    }
}
