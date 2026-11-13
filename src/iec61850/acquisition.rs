//! Akuisisi data per IED: report (RCB) dan/atau polling (LN + FC), lalu publish per titik.
//!
//! Satu thread OS per IED (fungsi libIEC61850 blocking). Report datang dari thread penerima
//! libIEC61850 lewat callback.

use std::ffi::{c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{info, warn};

use super::client::{fc_from_str, ConnectParams, Ied, Rcb, RcbSettings, Spec};
use super::mapper::{action_rank, mms_quality, mms_time, mms_to_json, reason_action};
use super::mapping::{split_fc, Point};
use crate::ffi;
use crate::publisher::Publisher;
use crate::repository::RtuConfig;

/// Nilai terakhir satu titik (gabungan value + q + t dari anggota-anggota dataset).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PointState {
    pub value: Value,
    pub q: Option<u16>,
    /// (ms epoch UTC, time quality)
    pub t: Option<(i64, u8)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Part {
    Value,
    Quality,
    Time,
}

/// Cara mengambil satu bagian (value/q/t) satu titik dari satu anggota dataset / objek yang dibaca.
struct Pick {
    point: usize,
    part: Part,
    /// None = nilai anggota itu sendiri; Some = path child format MMS (`mag$f`)
    path: Option<CString>,
}

/// Rencana decode untuk satu anggota (reference + FC). Dibuat sekali saat setup.
struct MemberPlan {
    spec: Option<Spec>,
    picks: Vec<Pick>,
}

/// Posisi `target` relatif terhadap `member`:
/// None = tidak berhubungan, Some(None) = sama persis, Some(Some(path)) = child (format `$`).
pub fn relative_path(member: &str, target: &str) -> Option<Option<String>> {
    if member == target {
        return Some(None);
    }
    let rest = target.strip_prefix(member)?.strip_prefix('.')?;
    Some(Some(rest.replace('.', "$")))
}

/// LN dari sebuah object reference: `LD/GGIO1.AnIn1` -> `LD/GGIO1`.
pub fn ln_of(reference: &str) -> &str {
    let slash = reference.find('/').unwrap_or(0);
    reference[slash..].find('.').map_or(reference, |i| &reference[..slash + i])
}

/// Apakah titik perlu dikirim di mode polling (dibanding nilai terakhir yang DIKIRIM)?
pub fn state_changed(prev: &PointState, cur: &PointState, deadband: f64) -> bool {
    if prev.q != cur.q {
        return true;
    }
    match (prev.value.as_f64(), cur.value.as_f64()) {
        (Some(a), Some(b)) if !a.is_nan() && !b.is_nan() => (b - a).abs() > deadband,
        _ => prev.value != cur.value,
    }
}

/// Jadwal siklus tetap; siklus yang terlewat dilewati (tidak ditumpuk).
pub fn next_deadline(deadline: Instant, now: Instant, interval: Duration) -> (Instant, u32) {
    let mut next = deadline + interval;
    let mut skipped = 0;
    while next <= now {
        next += interval;
        skipped += 1;
    }
    (next, skipped)
}

fn build_plan(ied: &Ied, points: &[Point], member_ref: &str, fc: &str) -> Result<MemberPlan, String> {
    let mut picks = Vec::new();
    for (i, p) in points.iter().enumerate().filter(|(_, p)| p.fc == fc) {
        for (part, attr) in [(Part::Value, p.value_attribute.as_str()), (Part::Quality, "q"), (Part::Time, "t")] {
            let target = format!("{}.{attr}", p.object_reference);
            if let Some(path) = relative_path(member_ref, &target) {
                let path = path.map(|s| CString::new(s).map_err(|_| "path mengandung NUL".to_string())).transpose()?;
                picks.push(Pick { point: i, part, path });
            }
        }
    }
    // Spec hanya perlu kalau ada bagian yang harus diambil dari dalam struktur.
    let spec = if picks.iter().any(|p| p.path.is_some()) {
        Some(ied.variable_spec(member_ref, fc_from_str(fc)?)?)
    } else {
        None
    };
    Ok(MemberPlan { spec, picks })
}

/// Tulis bagian-bagian titik dari `value` ke `state`. Kembalikan indeks titik yang tersentuh.
///
/// # Safety
/// `value` harus MmsValue valid sesuai tipe anggota ini.
unsafe fn decode(plan: &MemberPlan, value: *mut ffi::MmsValue, points: &[Point], state: &mut [PointState], touched: &mut Vec<usize>) {
    for pick in &plan.picks {
        let v = match (&pick.path, &plan.spec) {
            (None, _) => value,
            // SAFETY: value sesuai spec (syarat pemanggil)
            (Some(path), Some(spec)) => unsafe { spec.child(value, path) },
            (Some(_), None) => continue,
        };
        if v.is_null() {
            continue;
        }
        let s = &mut state[pick.point];
        // SAFETY: v child valid dari value
        unsafe {
            match pick.part {
                Part::Value => s.value = mms_to_json(v, &points[pick.point].cdc),
                Part::Quality => s.q = mms_quality(v),
                Part::Time => s.t = mms_time(v),
            }
        }
        if !touched.contains(&pick.point) {
            touched.push(pick.point);
        }
    }
}

/// Konteks satu IED, dipakai bersama oleh thread polling dan callback report.
pub struct RtuCtx {
    pub rtu: RtuConfig,
    pub rcbs: Vec<(String, RcbSettings)>,
    pub points: Vec<Point>,
    pub publisher: Arc<Publisher>,
    pub gate: Arc<ConnectGate>,
    /// true selama sesi MMS aktif (dipakai heartbeat RTU_STATUS)
    pub connected: AtomicBool,
    state: Mutex<Vec<PointState>>,
}

impl RtuCtx {
    pub fn new(
        rtu: RtuConfig,
        rcbs: Vec<(String, RcbSettings)>,
        points: Vec<Point>,
        publisher: Arc<Publisher>,
        gate: Arc<ConnectGate>,
    ) -> Self {
        let state = Mutex::new(vec![PointState::default(); points.len()]);
        Self { rtu, rcbs, points, publisher, gate, connected: AtomicBool::new(false), state }
    }

    fn publish(&self, i: usize, s: &PointState, action: &str) {
        self.publisher.publish(&self.rtu.ip_address, &self.points[i], s, action);
    }
}

struct ReportCtx {
    rtu: Arc<RtuCtx>,
    members: Vec<MemberPlan>,
}

unsafe extern "C" fn on_report(param: *mut c_void, report: ffi::ClientReport) {
    // Panic tidak boleh menyeberang ke C.
    let res = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: param = &ReportCtx yang hidup lebih lama dari koneksi (lihat session()).
        let ctx = unsafe { &*(param as *const ReportCtx) };
        let rtu = &ctx.rtu;
        // SAFETY: report & nilainya valid selama callback; tidak disimpan
        let values = unsafe { ffi::ClientReport_getDataSetValues(report) };
        if values.is_null() {
            return;
        }
        let Ok(mut state) = rtu.state.lock() else { return };
        let mut actions: Vec<(usize, &'static str)> = Vec::new();
        for (i, member) in ctx.members.iter().enumerate() {
            let reason = unsafe { ffi::ClientReport_getReasonForInclusion(report, i as i32) } as u32;
            if reason == ffi::IEC61850_REASON_NOT_INCLUDED {
                continue;
            }
            let action = reason_action(reason).unwrap_or("DATA_UPDATE");
            let v = unsafe { ffi::MmsValue_getElement(values, i as i32) };
            if v.is_null() {
                continue;
            }
            let mut touched = Vec::new();
            unsafe { decode(member, v, &rtu.points, &mut state, &mut touched) };
            for p in touched {
                match actions.iter_mut().find(|(q, _)| *q == p) {
                    Some(slot) if action_rank(action) > action_rank(slot.1) => slot.1 = action,
                    Some(_) => {}
                    None => actions.push((p, action)),
                }
            }
        }
        for (p, action) in actions {
            rtu.publish(p, &state[p], action);
        }
    }));
    if res.is_err() {
        warn!("panic di report handler (diabaikan)");
    }
}

struct PollGroup {
    reference: CString,
    fc: ffi::FunctionalConstraint,
    plan: MemberPlan,
}

/// Perintah ke thread IED (dari handler Kafka / ping).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Ctl {
    /// Hentikan thread (STATE false, DELETE, RESET, reload konfigurasi)
    Stop,
    /// GI sekarang (event GI). Mode polling: siklus integrity sekarang.
    Gi,
    /// Potong jeda reconnect (ping: IED kembali terjangkau)
    Wake,
}

/// Batas jumlah percobaan konek serentak (`MAX_CONCURRENT_CONNECTS`).
pub struct ConnectGate {
    free: Mutex<usize>,
    cv: Condvar,
}

pub struct GatePermit<'a>(&'a ConnectGate);

impl ConnectGate {
    pub fn new(max: usize) -> Self {
        Self { free: Mutex::new(max.max(1)), cv: Condvar::new() }
    }

    pub fn acquire(&self) -> GatePermit<'_> {
        let mut free = self.free.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            free = self.cv.wait(free).unwrap_or_else(|e| e.into_inner());
        }
        *free -= 1;
        GatePermit(self)
    }
}

impl Drop for GatePermit<'_> {
    fn drop(&mut self) {
        let mut free = self.0.free.lock().unwrap_or_else(|e| e.into_inner());
        *free += 1;
        self.0.cv.notify_one();
    }
}

enum End {
    /// Diminta berhenti lewat Ctl::Stop
    Stopped,
    /// Koneksi putus setelah sempat jalan
    Disconnected,
}

/// Loop: konek, akuisisi sampai putus, tunggu `reconnect_time_ms`, ulangi. Berhenti saat Ctl::Stop.
/// `reconnect_retry_count` = 0 berarti tanpa batas.
pub fn run_ied(ctx: Arc<RtuCtx>, rx: Receiver<Ctl>) {
    let r = &ctx.rtu;
    let producer = &ctx.publisher.producer;
    let mut failures: u32 = 0;
    loop {
        let result = session(&ctx, &rx);
        let was_connected = ctx.connected.swap(false, Ordering::SeqCst);
        if was_connected {
            producer.send_status(&r.ip_address, r.id_rtu, 0);
        }
        match result {
            Ok(End::Stopped) => {
                info!(id_rtu = r.id_rtu, "thread IED dihentikan");
                return;
            }
            Ok(End::Disconnected) => {
                failures = 0;
                warn!(id_rtu = r.id_rtu, "koneksi ke IED terputus");
                producer.send_log(&r.ip_address, r.id_rtu, "WARN", "koneksi MMS terputus");
            }
            Err(e) => {
                failures += 1;
                warn!(id_rtu = r.id_rtu, attempt = failures, "sesi IED gagal: {e}");
                // Status 0 + log hanya di kegagalan pertama supaya tidak banjir (heartbeat tetap jalan).
                if failures == 1 {
                    if !was_connected {
                        producer.send_status(&r.ip_address, r.id_rtu, 0);
                    }
                    producer.send_log(&r.ip_address, r.id_rtu, "ERROR", &format!("gagal konek MMS: {e}"));
                }
                let max = r.reconnect_retry_count;
                if max > 0 && failures >= max {
                    warn!(id_rtu = r.id_rtu, "berhenti reconnect setelah {failures} percobaan");
                    producer.send_log(&r.ip_address, r.id_rtu, "ERROR", "berhenti reconnect, batas percobaan tercapai");
                    return;
                }
            }
        }
        match rx.recv_timeout(Duration::from_millis(r.reconnect_time_ms)) {
            Ok(Ctl::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Ok(Ctl::Gi | Ctl::Wake) | Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

/// Satu sesi koneksi.
fn session(ctx: &Arc<RtuCtx>, rx: &Receiver<Ctl>) -> Result<End, String> {
    let r = &ctx.rtu;
    let producer = &ctx.publisher.producer;
    // Dideklarasikan SEBELUM `ied` -> di-drop SESUDAH koneksi ditutup, jadi callback tidak
    // pernah memegang ReportCtx yang sudah dibebaskan.
    let mut report_ctxs: Vec<Box<ReportCtx>> = Vec::new();
    let ied = {
        let _permit = ctx.gate.acquire();
        Ied::connect(&ConnectParams {
            host: &r.ip_address,
            port: r.port,
            local_ip: Some(r.local_ip_address.as_str()),
            connect_timeout_ms: r.connect_timeout_ms,
            request_timeout_ms: r.request_timeout_ms,
        })?
    };
    ctx.connected.store(true, Ordering::SeqCst);
    producer.send_status(&r.ip_address, r.id_rtu, 1);
    let msg = format!("terkoneksi MMS {}:{} mode {}", r.ip_address, r.port, r.acquisition_mode);
    producer.send_log(&r.ip_address, r.id_rtu, "INFO", &msg);
    info!(id_rtu = r.id_rtu, "{msg}");

    let use_report = matches!(r.acquisition_mode.as_str(), "REPORT" | "HYBRID");
    let use_poll = matches!(r.acquisition_mode.as_str(), "POLLING" | "HYBRID");

    let mut rcbs = Vec::new();
    if use_report {
        for (rcb_ref, settings) in &ctx.rcbs {
            match setup_rcb(&ied, ctx, rcb_ref, settings, &mut report_ctxs) {
                Ok(rcb) => rcbs.push(rcb),
                Err(e) => {
                    warn!(id_rtu = r.id_rtu, "RCB {rcb_ref} dilewati: {e}");
                    producer.send_log(&r.ip_address, r.id_rtu, "WARN", &format!("RCB {rcb_ref} gagal: {e}"));
                }
            }
        }
    }

    let mut groups = Vec::new();
    if use_poll {
        // HYBRID: status lewat report, polling hanya measurement (MX).
        let mut keys: Vec<(String, String)> = ctx
            .points
            .iter()
            .filter(|p| r.acquisition_mode != "HYBRID" || p.fc == "MX")
            .filter(|p| p.fc != "CO")
            .map(|p| (ln_of(&p.object_reference).to_string(), p.fc.clone()))
            .collect();
        keys.sort();
        keys.dedup();
        for (ln, fc) in keys {
            let group = build_plan(&ied, &ctx.points, &ln, &fc).and_then(|plan| {
                Ok(PollGroup { reference: CString::new(ln.clone()).map_err(|_| "NUL")?, fc: fc_from_str(&fc)?, plan })
            });
            match group {
                Ok(g) => groups.push(g),
                Err(e) => warn!(id_rtu = r.id_rtu, "polling {ln}[{fc}] dilewati: {e}"),
            }
        }
    }

    // 0 = hanya GI awal (tanpa GI berkala)
    let gi_every = (r.general_interrogation_interval > 0).then(|| Duration::from_secs(r.general_interrogation_interval));
    let interval = Duration::from_millis(r.poll_interval_ms);
    let integrity = Duration::from_millis(r.integrity_period_ms);
    let mut last_gi: Option<Instant> = None;
    let mut next_poll = Instant::now();
    let mut last_integrity: Option<Instant> = None;
    let mut sent: Vec<Option<PointState>> = vec![None; ctx.points.len()];

    loop {
        let gi_due = last_gi.is_none_or(|t| gi_every.is_some_and(|every| t.elapsed() >= every));
        if !rcbs.is_empty() && gi_due {
            for rcb in &rcbs {
                if let Err(e) = rcb.gi(&ied) {
                    warn!(id_rtu = r.id_rtu, "GI {} gagal: {e}", rcb.reference);
                }
            }
            last_gi = Some(Instant::now());
        }

        if !groups.is_empty() && Instant::now() >= next_poll {
            let is_integrity = last_integrity.is_none_or(|t| t.elapsed() >= integrity);
            if is_integrity {
                last_integrity = Some(Instant::now());
            }
            poll_cycle(&ied, ctx, &groups, &mut sent, is_integrity);
            let (next, skipped) = next_deadline(next_poll, Instant::now(), interval);
            if skipped > 0 {
                warn!(id_rtu = r.id_rtu, "polling terlambat, {skipped} siklus dilewati");
            }
            next_poll = next;
        }

        if !ied.is_connected() {
            return Ok(End::Disconnected);
        }
        let wait = if groups.is_empty() {
            Duration::from_millis(200)
        } else {
            next_poll.saturating_duration_since(Instant::now()).min(Duration::from_millis(200))
        };
        match rx.recv_timeout(wait) {
            Ok(Ctl::Stop) | Err(RecvTimeoutError::Disconnected) => return Ok(End::Stopped),
            Ok(Ctl::Gi) => {
                info!(id_rtu = r.id_rtu, "GI diminta");
                last_gi = None;
                last_integrity = None;
                next_poll = Instant::now();
            }
            Ok(Ctl::Wake) | Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

// Box WAJIB: callback C memegang pointer mentah ke ReportCtx. Tanpa Box, Vec memindahkan
// elemen saat realokasi dan pointer itu jadi menggantung (use-after-free).
#[allow(clippy::vec_box)]
fn setup_rcb(ied: &Ied, ctx: &Arc<RtuCtx>, rcb_ref: &str, settings: &RcbSettings, out: &mut Vec<Box<ReportCtx>>) -> Result<Rcb, String> {
    let rcb = Rcb::read(ied, rcb_ref)?;
    let ds = settings.dataset.clone().or_else(|| rcb.dataset()).ok_or("RCB tidak punya dataset")?;
    let mut members = Vec::new();
    for m in ied.dataset_directory(&ds)? {
        let (reference, fc) = split_fc(&m).ok_or_else(|| format!("anggota tanpa FC: {m}"))?;
        members.push(build_plan(ied, &ctx.points, reference, fc)?);
    }
    let mapped = members.iter().filter(|m| !m.picks.is_empty()).count();
    info!(id_rtu = ctx.rtu.id_rtu, "RCB {rcb_ref}: dataset {ds}, {} anggota, {mapped} termapping", members.len());

    let rc = Box::new(ReportCtx { rtu: Arc::clone(ctx), members });
    let param = &*rc as *const ReportCtx as *mut c_void;
    out.push(rc);
    // SAFETY: ReportCtx di heap, hidup sampai setelah `ied` di-drop (lihat session()).
    unsafe { rcb.install_handler(ied, Some(on_report), param) };
    rcb.enable(ied, settings)?;
    Ok(rcb)
}

fn poll_cycle(ied: &Ied, ctx: &RtuCtx, groups: &[PollGroup], sent: &mut [Option<PointState>], is_integrity: bool) {
    // Satu siklus berurutan: satu request MMS per (LN, FC). Dibaca dulu TANPA lock supaya
    // callback report (mode HYBRID) tidak tertahan selama request berjalan.
    let values: Vec<_> = groups
        .iter()
        .filter_map(|g| match ied.read_object(&g.reference, g.fc) {
            Ok(v) => Some((g, v)),
            Err(e) => {
                warn!(id_rtu = ctx.rtu.id_rtu, "polling {:?} gagal: {e}", g.reference);
                None
            }
        })
        .collect();

    let Ok(mut state) = ctx.state.lock() else { return };
    let mut touched = Vec::new();
    for (g, v) in &values {
        // SAFETY: nilai hasil readObject sesuai spec LN+FC yang sama
        unsafe { decode(&g.plan, v.as_ptr(), &ctx.points, &mut state, &mut touched) };
    }
    for i in touched {
        let cur = &state[i];
        let deadband = ctx.points[i].deadband.unwrap_or(0.0);
        let action = match &sent[i] {
            _ if is_integrity => "INTEGRITY",
            None => "POLLING",
            Some(prev) if state_changed(prev, cur, deadband) => "POLLING",
            Some(_) => continue,
        };
        ctx.publish(i, cur, action);
        sent[i] = Some(cur.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn path_relatif() {
        assert_eq!(relative_path("LD/G.AnIn1.mag.f", "LD/G.AnIn1.mag.f"), Some(None));
        assert_eq!(relative_path("LD/G.SPCSO1", "LD/G.SPCSO1.stVal"), Some(Some("stVal".into())));
        assert_eq!(relative_path("LD/G", "LD/G.AnIn1.mag.f"), Some(Some("AnIn1$mag$f".into())));
        assert_eq!(relative_path("LD/G.AnIn1.q", "LD/G.AnIn1.mag.f"), None);
        // AnIn1 bukan prefix dari AnIn10
        assert_eq!(relative_path("LD/G.AnIn1", "LD/G.AnIn10.mag.f"), None);
    }

    #[test]
    fn ln_dari_reference() {
        assert_eq!(ln_of("LD/GGIO1.AnIn1"), "LD/GGIO1");
        assert_eq!(ln_of("LD/MMXU1.PhV.phsA"), "LD/MMXU1");
        assert_eq!(ln_of("LD.x/LLN0.Mod"), "LD.x/LLN0");
        assert_eq!(ln_of("LD/LLN0"), "LD/LLN0");
    }

    #[test]
    fn perubahan_polling() {
        let s = |v: Value, q: Option<u16>| PointState { value: v, q, t: None };
        assert!(!state_changed(&s(json!(1.0), Some(0)), &s(json!(1.04), Some(0)), 0.05));
        assert!(state_changed(&s(json!(1.0), Some(0)), &s(json!(1.1), Some(0)), 0.05));
        assert!(state_changed(&s(json!(1.0), Some(0)), &s(json!(1.0), Some(2)), 0.05)); // q berubah
        assert!(state_changed(&s(json!(1), None), &s(json!(2), None), 0.0)); // DPS off->on
        assert!(!state_changed(&s(json!(2), None), &s(json!(2), None), 0.0));
        assert!(state_changed(&s(Value::Null, None), &s(json!(1), None), 0.0));
    }

    #[test]
    fn gate_membatasi_koneksi_serentak() {
        let gate = Arc::new(ConnectGate::new(2));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (gate, active, peak) = (Arc::clone(&gate), Arc::clone(&active), Arc::clone(&peak));
                std::thread::spawn(move || {
                    let _p = gate.acquire();
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(20));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn jadwal_lewati_siklus() {
        let i = Duration::from_millis(100);
        let d = Instant::now();
        assert_eq!(next_deadline(d, d + Duration::from_millis(50), i), (d + i, 0));
        assert_eq!(next_deadline(d, d + Duration::from_millis(250), i), (d + 3 * i, 2));
    }
}
