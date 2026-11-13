//! Akuisisi data per IED: report (RCB) dan/atau polling (LN + FC), lalu publish per titik.
//!
//! Satu thread OS per IED (fungsi libIEC61850 blocking). Report datang dari thread penerima
//! libIEC61850 lewat callback.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::{c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{error, info, warn};

use super::client::{
    control_model_from_str, fc_from_str, octet_bytes, osi_params, ConnectParams, Control, Ied, Liveness, OsiParams,
    OwnedValue, Rcb, RcbSettings, Spec,
};
use crate::command::{ctl_value, CommandAction, CommandJob, CommandReq, CtlVal};
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

/// Batas tunggu CommandTermination: kolom oper_timeout_ms titik, NULL = env OPER_TIMEOUT_MS.
pub fn oper_wait(point_ms: Option<i32>, default_ms: u64) -> Duration {
    Duration::from_millis(point_ms.filter(|&v| v > 0).map_or(default_ms, |v| v as u64))
}

/// Target cek liveness: `LD/LLN0.Mod.stVal` dari LD pertama (titik termapping, lalu RCB).
pub fn probe_reference(points: &[Point], rcbs: &[(String, RcbSettings)]) -> Option<String> {
    points
        .iter()
        .map(|p| p.object_reference.as_str())
        .chain(rcbs.iter().map(|(r, _)| r.as_str()))
        .find_map(|r| r.split_once('/').map(|(ld, _)| format!("{ld}/LLN0.Mod.stVal")))
}

/// Interval GI berkala. 0 = hanya GI awal setelah konek.
pub fn gi_every(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

/// Coba setiap item `pending`; yang berhasil dikeluarkan dan dikembalikan, yang gagal tetap tinggal.
pub fn retry_pending<T: Copy, R>(pending: &mut Vec<T>, mut f: impl FnMut(T) -> Option<R>) -> Vec<R> {
    let mut ok = Vec::new();
    pending.retain(|&t| match f(t) {
        Some(r) => {
            ok.push(r);
            false
        }
        None => true,
    });
    ok
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
    /// Di-set Manager saat stop: thread yang masih antre di ConnectGate tidak jadi konek.
    pub stopping: AtomicBool,
    /// Parameter OSI tervalidasi (None = default libIEC61850, OSI_PARAMS_SIDE=none)
    pub osi: Option<OsiParams>,
    state: Mutex<Vec<PointState>>,
    /// EntryID report terakhir per RCB (BRCB). Hidup selama RtuCtx, jadi bertahan saat reconnect.
    // ponytail: hilang saat reload/restart FEP; simpan di DB kalau event selama restart FEP juga perlu
    entry_ids: Mutex<HashMap<String, Vec<u8>>>,
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
        let (osi, errors) = osi_params(rtu.osi_side, &rtu.ap_title, rtu.ae_qualifier, &rtu.p_selector, &rtu.s_selector, &rtu.t_selector);
        for e in &errors {
            error!(id_rtu = rtu.id_rtu, "parameter OSI: {e}");
            publisher.producer.send_log(&rtu.ip_address, rtu.id_rtu, "ERROR", &format!("parameter OSI: {e}"));
        }
        Self {
            rtu,
            rcbs,
            points,
            publisher,
            gate,
            connected: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            osi,
            state,
            entry_ids: Mutex::default(),
        }
    }

    fn publish(&self, i: usize, s: &PointState, action: &str, entry_time: Option<i64>) {
        self.publisher.publish(&self.rtu.ip_address, &self.points[i], s, action, entry_time);
    }

    fn entry_id(&self, rcb_ref: &str) -> Option<Vec<u8>> {
        self.entry_ids.lock().ok()?.get(rcb_ref).cloned()
    }

    fn set_entry_id(&self, rcb_ref: &str, id: Vec<u8>) {
        if let Ok(mut m) = self.entry_ids.lock() {
            m.insert(rcb_ref.to_string(), id);
        }
    }

    fn forget_entry_id(&self, rcb_ref: &str) {
        if let Ok(mut m) = self.entry_ids.lock() {
            m.remove(rcb_ref);
        }
    }
}

struct ReportCtx {
    rtu: Arc<RtuCtx>,
    rcb_ref: String,
    members: Vec<MemberPlan>,
    /// Report masuk = tanda hidup koneksi
    alive: Arc<Liveness>,
}

unsafe extern "C" fn on_report(param: *mut c_void, report: ffi::ClientReport) {
    // Panic tidak boleh menyeberang ke C.
    let res = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: param = &ReportCtx yang hidup lebih lama dari koneksi (lihat session()).
        let ctx = unsafe { &*(param as *const ReportCtx) };
        ctx.alive.touch();
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
        // Waktu entri report: dipakai untuk titik yang tidak punya atribut `t`.
        // SAFETY: report valid selama callback
        let entry_time = unsafe { ffi::ClientReport_hasTimestamp(report) }
            .then(|| unsafe { ffi::ClientReport_getTimestamp(report) } as i64)
            .filter(|&ms| ms > 0);
        for (p, action) in actions {
            rtu.publish(p, &state[p], action, entry_time);
        }
        drop(state);
        // EntryID dicatat SETELAH report diproses: saat reconnect IED melanjutkan dari entri berikutnya.
        // SAFETY: nilai milik report, langsung disalin
        if let Some(id) = unsafe { octet_bytes(ffi::ClientReport_getEntryId(report)) } {
            rtu.set_entry_id(&ctx.rcb_ref, id);
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
pub enum Ctl {
    /// Hentikan thread (STATE false, DELETE, RESET, reload konfigurasi)
    Stop,
    /// GI sekarang (event GI). Mode polling: siklus integrity sekarang.
    Gi,
    /// Potong jeda reconnect (ping: IED kembali terjangkau)
    Wake,
    /// Select / operate / cancel (event COMMAND). Hasil dikirim lewat `reply`.
    Command(Box<CommandJob>),
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
            Ok(Ctl::Command(job)) => {
                let _ = job.reply.send(Err("IED tidak terkoneksi".into()));
            }
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
        if ctx.stopping.load(Ordering::SeqCst) {
            return Ok(End::Stopped);
        }
        Ied::connect(&ConnectParams {
            host: &r.ip_address,
            port: r.port,
            local_ip: Some(r.local_ip_address.as_str()),
            connect_timeout_ms: r.connect_timeout_ms,
            request_timeout_ms: r.request_timeout_ms,
            osi: ctx.osi.as_ref(),
        })?
    };
    ctx.connected.store(true, Ordering::SeqCst);
    producer.send_status(&r.ip_address, r.id_rtu, 1);
    let msg = format!("terkoneksi MMS {}:{} mode {}", r.ip_address, r.port, r.acquisition_mode);
    producer.send_log(&r.ip_address, r.id_rtu, "INFO", &msg);
    info!(id_rtu = r.id_rtu, "{msg}");

    let use_report = matches!(r.acquisition_mode.as_str(), "REPORT" | "HYBRID");
    let use_poll = matches!(r.acquisition_mode.as_str(), "POLLING" | "HYBRID");

    // RCB yang gagal (mis. masih di-reserve sesi lama / IED belum siap) dicoba ulang tiap RCB_RETRY_SECS.
    // Koneksi mati di tengah setup -> RCB berikutnya tidak dicoba di koneksi yang sama.
    let rcb_retry = Duration::from_secs(r.rcb_retry_secs);
    let mut pending: Vec<usize> = if use_report { (0..ctx.rcbs.len()).collect() } else { Vec::new() };
    let mut rcbs = retry_pending(&mut pending, |i| (!ied.lost()).then(|| try_setup_rcb(&ied, ctx, i, &mut report_ctxs, false)).flatten());
    let mut next_rcb_retry = Instant::now() + rcb_retry;
    if ied.lost() {
        return Ok(End::Disconnected);
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

    if ied.lost() {
        return Ok(End::Disconnected);
    }

    // Liveness: tanpa request sukses / report selama LIVENESS_SECS -> baca LLN0.Mod.stVal.
    // Koneksi bisa mati tapi getState() tetap CONNECTED (lihat docs/CATATAN_TERBUKA.md).
    let liveness = Duration::from_secs(r.liveness_secs);
    let probe_ref = probe_reference(&ctx.points, &ctx.rcbs).and_then(|s| CString::new(s).ok());

    let gi_every = gi_every(r.general_interrogation_interval);
    let interval = Duration::from_millis(r.poll_interval_ms);
    let integrity = Duration::from_millis(r.integrity_period_ms);
    let mut last_gi: Option<Instant> = None;
    let mut next_poll = Instant::now();
    let mut last_integrity: Option<Instant> = None;
    let mut sent: Vec<Option<PointState>> = vec![None; ctx.points.len()];
    let mut controls: HashMap<i32, Control> = HashMap::new();

    loop {
        let gi_due = last_gi.is_none_or(|t| gi_every.is_some_and(|every| t.elapsed() >= every));
        if !rcbs.is_empty() && gi_due {
            for rcb in rcbs.iter().take_while(|_| !ied.lost()) {
                if let Err(e) = rcb.gi(&ied) {
                    warn!(id_rtu = r.id_rtu, "GI {} gagal: {e}", rcb.reference);
                }
            }
            last_gi = Some(Instant::now());
        }

        if !pending.is_empty() && !ied.lost() && Instant::now() >= next_rcb_retry {
            let recovered =
                retry_pending(&mut pending, |i| (!ied.lost()).then(|| try_setup_rcb(&ied, ctx, i, &mut report_ctxs, true)).flatten());
            // GI supaya nilai terkini langsung terkirim, tidak menunggu GI berkala.
            // last_gi None = GI awal belum jalan, nanti otomatis mencakup RCB ini.
            for rcb in recovered.iter().filter(|_| last_gi.is_some()) {
                if let Err(e) = rcb.gi(&ied) {
                    warn!(id_rtu = r.id_rtu, "GI {} gagal: {e}", rcb.reference);
                }
            }
            rcbs.extend(recovered);
            next_rcb_retry = Instant::now() + rcb_retry;
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

        if !ied.lost() && ied.idle() >= liveness {
            if let Some(probe) = &probe_ref {
                if let Err(e) = ied.probe(probe) {
                    let msg = format!("tidak ada respons IED {}s, cek {probe:?} gagal: {e}; reconnect", liveness.as_secs());
                    warn!(id_rtu = r.id_rtu, "{msg}");
                    producer.send_log(&r.ip_address, r.id_rtu, "WARN", &msg);
                    return Ok(End::Disconnected);
                }
            }
        }

        if !ied.is_connected() || ied.lost() {
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
            Ok(Ctl::Command(job)) => {
                let result = execute_command(&ied, ctx, &mut controls, &job.req);
                let msg = match &result {
                    Ok(()) => format!("{:?} ioa {} sukses", job.req.action, job.req.ioa),
                    Err(e) => format!("{:?} ioa {} gagal: {e}", job.req.action, job.req.ioa),
                };
                producer.send_log(&r.ip_address, r.id_rtu, if result.is_ok() { "INFO" } else { "WARN" }, &msg);
                let _ = job.reply.send(result);
            }
            Ok(Ctl::Wake) | Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

/// Jalankan select/operate/cancel di thread IED. Objek kontrol di-cache per IOA selama sesi,
/// supaya SELECT dan OPERATE yang datang terpisah memakai objek (ctlNum) yang sama.
fn execute_command(ied: &Ied, ctx: &RtuCtx, controls: &mut HashMap<i32, Control>, req: &CommandReq) -> Result<(), String> {
    let p = ctx
        .points
        .iter()
        .find(|p| p.ioa == req.ioa && p.fc == "CO")
        .ok_or_else(|| format!("IOA {} bukan titik kontrol (FC CO) di RTU ini", req.ioa))?;

    let ctl = match controls.entry(req.ioa) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => {
            let ctl = Control::create(ied, &p.object_reference)?;
            if let Some(m) = p.ctl_model.as_deref().and_then(control_model_from_str) {
                ctl.set_control_model(m);
            }
            e.insert(ctl)
        }
    };
    ctl.prepare(&req.or_ident, p.interlock_check, p.synchro_check)?;

    let value = || -> Result<OwnedValue, String> {
        let v = req.value.ok_or("value wajib diisi")?;
        Ok(match ctl_value(&p.cdc, v)? {
            CtlVal::Bool(b) => OwnedValue::boolean(b),
            CtlVal::Int(i) => OwnedValue::int32(i),
        })
    };
    let result = match req.action {
        CommandAction::Select => ctl.select(&value()?),
        CommandAction::Operate => ctl.operate(&value()?, oper_wait(p.oper_timeout_ms, ctx.rtu.oper_timeout_ms)),
        CommandAction::Cancel => ctl.cancel(),
    };
    if result.is_ok() {
        ied.touch();
    } else if ctl.connection_lost() {
        // sesi diakhiri di loop session() -> reconnect
        ied.mark_lost();
    }
    result
}

/// Setup RCB ke-`i` + log. Gagal pertama: WARN ke RTU_LOGGING. Percobaan ulang yang gagal hanya
/// di log lokal (supaya RTU_LOGGING tidak banjir); yang berhasil: INFO ke RTU_LOGGING.
#[allow(clippy::vec_box)] // lihat setup_rcb
fn try_setup_rcb(ied: &Ied, ctx: &Arc<RtuCtx>, i: usize, out: &mut Vec<Box<ReportCtx>>, retry: bool) -> Option<Rcb> {
    let r = &ctx.rtu;
    let (rcb_ref, settings) = &ctx.rcbs[i];
    let producer = &ctx.publisher.producer;
    match setup_rcb(ied, ctx, rcb_ref, settings, out) {
        Ok(rcb) => {
            if retry {
                info!(id_rtu = r.id_rtu, "RCB {rcb_ref} aktif setelah dicoba ulang");
                producer.send_log(&r.ip_address, r.id_rtu, "INFO", &format!("RCB {rcb_ref} aktif setelah dicoba ulang"));
            } else {
                info!(id_rtu = r.id_rtu, "RCB {rcb_ref} aktif");
            }
            Some(rcb)
        }
        Err(e) if retry => {
            warn!(id_rtu = r.id_rtu, "RCB {rcb_ref} masih gagal: {e}");
            None
        }
        Err(e) => {
            let msg = format!("RCB {rcb_ref} gagal: {e}; dicoba ulang tiap {} detik", r.rcb_retry_secs);
            warn!(id_rtu = r.id_rtu, "{msg}");
            producer.send_log(&r.ip_address, r.id_rtu, "WARN", &msg);
            None
        }
    }
}

/// BRCB setelah reconnect: tulis EntryID terakhir supaya IED mengirim entri buffer SESUDAHNYA
/// (event selama putus, tanpa dobel). Ditolak -> lupakan EntryID, enable biasa, WARN ke RTU_LOGGING.
fn resume_from_entry_id(ied: &Ied, ctx: &RtuCtx, rcb: &Rcb) {
    let r = &ctx.rtu;
    let Some(id) = ctx.entry_id(&rcb.reference) else { return };
    match rcb.write_entry_id(ied, &id) {
        Ok(()) => info!(id_rtu = r.id_rtu, "RCB {}: lanjut dari EntryID {}", rcb.reference, hex(&id)),
        Err(e) => {
            ctx.forget_entry_id(&rcb.reference);
            let msg = format!("RCB {}: EntryID {} ditolak IED ({e}), enable tanpa EntryID; event selama putus bisa hilang", rcb.reference, hex(&id));
            warn!(id_rtu = r.id_rtu, "{msg}");
            ctx.publisher.producer.send_log(&r.ip_address, r.id_rtu, "WARN", &msg);
        }
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
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

    let rc = Box::new(ReportCtx { rtu: Arc::clone(ctx), rcb_ref: rcb_ref.to_string(), members, alive: ied.liveness() });
    let param = &*rc as *const ReportCtx as *mut c_void;
    out.push(rc);
    // SAFETY: ReportCtx di heap, hidup sampai setelah `ied` di-drop (lihat session()).
    unsafe { rcb.install_handler(ied, Some(on_report), param) };
    if rcb.is_buffered() {
        resume_from_entry_id(ied, ctx, &rcb);
    }
    if let Err(e) = rcb.enable(ied, settings, ctx.rtu.brcb_resv_tms_secs) {
        // Lepas handler dulu, baru buang ReportCtx-nya: setelah uninstall tidak ada callback
        // yang memegang pointer ini. Tanpa ini tiap percobaan ulang menumpuk satu ReportCtx.
        rcb.uninstall_handler(ied);
        out.pop();
        return Err(e);
    }
    Ok(rcb)
}

fn poll_cycle(ied: &Ied, ctx: &RtuCtx, groups: &[PollGroup], sent: &mut [Option<PointState>], is_integrity: bool) {
    // Satu siklus berurutan: satu request MMS per (LN, FC). Dibaca dulu TANPA lock supaya
    // callback report (mode HYBRID) tidak tertahan selama request berjalan.
    let values: Vec<_> = groups
        .iter()
        .take_while(|_| !ied.lost())
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
        ctx.publish(i, cur, action, None);
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
    fn rcb_gagal_dicoba_ulang_sampai_berhasil() {
        let mut pending = vec![0, 1, 2];
        let mut attempts = [0; 3];
        // RCB 1 baru berhasil di percobaan ke-2 (mis. reservasi sesi lama sudah lepas), RCB 2 tidak pernah.
        let mut setup = |i: usize| {
            attempts[i] += 1;
            (i == 0 || (i == 1 && attempts[1] >= 2)).then_some(i)
        };
        assert_eq!(retry_pending(&mut pending, &mut setup), vec![0]);
        assert_eq!(pending, vec![1, 2]);
        assert_eq!(retry_pending(&mut pending, &mut setup), vec![1]);
        assert_eq!(pending, vec![2]);
        assert!(retry_pending(&mut pending, &mut setup).is_empty());
        assert_eq!(attempts, [1, 2, 3]);
    }

    #[test]
    fn target_liveness() {
        use crate::iec61850::mapping::default_value_attribute;
        let pt = |r: &str| Point {
            id_rtu: 1,
            ioa: 1,
            tag: None,
            object_reference: r.into(),
            fc: "ST".into(),
            cdc: "SPS".into(),
            value_attribute: default_value_attribute("SPS").unwrap_or_default().into(),
            ctl_model: None,
            sbo_timeout_ms: None,
            interlock_check: false,
            synchro_check: false,
            deadband: None,
            oper_timeout_ms: None,
        };
        let rcb = |r: &str| (r.to_string(), RcbSettings::default());
        assert_eq!(probe_reference(&[pt("BAY1LD0/XCBR1.Pos")], &[rcb("X/LLN0.BR.b")]).as_deref(), Some("BAY1LD0/LLN0.Mod.stVal"));
        // tanpa titik: dari RCB
        assert_eq!(probe_reference(&[], &[rcb("simpleIOGenericIO/LLN0.BR.EventsBRCB01")]).as_deref(), Some("simpleIOGenericIO/LLN0.Mod.stVal"));
        assert_eq!(probe_reference(&[pt("tanpa-slash")], &[]), None);
        assert_eq!(probe_reference(&[], &[]), None);
    }

    #[test]
    fn tunggu_command_termination() {
        assert_eq!(oper_wait(Some(3000), 10_000), Duration::from_millis(3000));
        assert_eq!(oper_wait(None, 10_000), Duration::from_millis(10_000)); // NULL -> env
        assert_eq!(oper_wait(Some(0), 7000), Duration::from_millis(7000));
    }

    #[tokio::test]
    async fn entry_id_dan_osi_di_ctx() {
        use crate::iec61850::client::{OsiSide, DEFAULT_T_SELECTOR};
        let mgr = crate::manager::testing::manager();
        let mut rtu = crate::manager::testing::rtu(1);
        rtu.osi_side = OsiSide::Remote;
        rtu.t_selector = "zz".into(); // tidak valid -> default + log ERROR
        let ctx = RtuCtx::new(rtu, vec![], vec![], Arc::clone(&mgr.publisher), Arc::new(ConnectGate::new(1)));
        let osi = ctx.osi.as_ref().unwrap();
        assert_eq!((osi.side, osi.t_selector.clone()), (OsiSide::Remote, DEFAULT_T_SELECTOR.to_vec()));
        assert_eq!(osi.ap_title.as_deref(), Some("1.1.999.1"));

        // EntryID disimpan di RtuCtx (hidup lintas sesi/reconnect), per RCB
        let brcb = "LD/LLN0.BR.brcb01";
        assert_eq!(ctx.entry_id(brcb), None);
        ctx.set_entry_id(brcb, vec![1; 8]);
        ctx.set_entry_id(brcb, vec![2; 8]); // entri terbaru menimpa
        assert_eq!(ctx.entry_id(brcb), Some(vec![2; 8]));
        assert_eq!(ctx.entry_id("LD/LLN0.BR.brcb02"), None);
        ctx.forget_entry_id(brcb); // ditolak IED
        assert_eq!(ctx.entry_id(brcb), None);
    }

    #[test]
    fn entry_id_hex() {
        assert_eq!(hex(&[0, 1, 0xab, 0xff]), "0001ABFF");
    }

    #[test]
    fn interval_gi() {
        assert_eq!(gi_every(0), None); // hanya GI awal
        assert_eq!(gi_every(300), Some(Duration::from_secs(300)));
    }

    #[test]
    fn jadwal_lewati_siklus() {
        let i = Duration::from_millis(100);
        let d = Instant::now();
        assert_eq!(next_deadline(d, d + Duration::from_millis(50), i), (d + i, 0));
        assert_eq!(next_deadline(d, d + Duration::from_millis(250), i), (d + 3 * i, 2));
    }
}
