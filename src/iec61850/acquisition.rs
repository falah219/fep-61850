//! Akuisisi data per IED: report (RCB) dan/atau polling (LN + FC), lalu publish per titik.
//!
//! Satu thread OS per IED (fungsi libIEC61850 blocking). Report datang dari thread penerima
//! libIEC61850 lewat callback.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::{c_void, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use super::client::{
    control_model_from_str, fc_from_str, fc_name, is_connection_error, osi_params, ConnectParams, Control, Ied, OsiParams,
    OwnedValue, Rcb, RcbSettings, Spec, SpecNode,
};
use super::callbacks::report::{on_report, ReportCtx};
use super::mapper::{mms_quality, mms_time, mms_to_json};
use super::mapping::split_fc;
use crate::connection::ConnectGate;
use super::browse::{ctl_model_name, BrowseJob, BrowseOptions, BrowseResult};
use super::traffic::{self, TrafficLog};
use super::unmapped;
use crate::domain::{ctl_value, CommandAction, CommandJob, CommandReq, CtlVal, Point, PointState, RtuConfig};
use crate::error::Result;
use crate::ffi;
use crate::kafka::publisher::{Origin, Publisher};

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
pub(crate) struct MemberPlan {
    spec: Option<Spec>,
    picks: Vec<Pick>,
    /// Bagian titik virtual (`ReportCtx::unmapped`) kalau anggota ini belum punya titik (RTU_DATAS_UNMAPPED)
    unmapped: Vec<Pick>,
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

/// Bagian value / q / t titik-titik `points` yang ada di anggota `member_ref` [fc].
fn picks_for(points: &[Point], member_ref: &str, fc: &str) -> Result<Vec<Pick>> {
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
    Ok(picks)
}

fn build_plan(ied: &Ied, points: &[Point], member_ref: &str, fc: &str) -> Result<MemberPlan> {
    let picks = picks_for(points, member_ref, fc)?;
    // Spec hanya perlu kalau ada bagian yang harus diambil dari dalam struktur.
    let spec = if picks.iter().any(|p| p.path.is_some()) {
        Some(ied.variable_spec(member_ref, fc_from_str(fc)?)?)
    } else {
        None
    };
    Ok(MemberPlan { spec, picks, unmapped: Vec::new() })
}

/// Anggota yang diteruskan tanpa IOA: belum punya titik. RTU_DATAS_UNMAPPED=false -> tidak ada (perilaku lama).
fn unmapped_candidates(enabled: bool, members: &[MemberPlan]) -> Vec<usize> {
    if !enabled {
        return Vec::new();
    }
    members.iter().enumerate().filter(|(_, m)| m.picks.is_empty()).map(|(i, _)| i).collect()
}

/// Titik virtual untuk anggota `todo` (lihat `unmapped`): struktur dari getVariableSpecification (spec disimpan
/// untuk decode). Anggota yang gagal dibaca / strukturnya tidak dikenal dilewati (DEBUG), report tetap jalan.
// ponytail: satu getVariableSpecification per anggota tanpa titik saat setup RCB; cache per (reference, FC) kalau
// dataset besar membuat setup RCB lambat
fn plan_unmapped(ied: &Ied, id_rtu: i32, refs: &[(String, String)], members: &mut [MemberPlan], todo: Vec<usize>) -> Result<Vec<Point>> {
    let mut found: Vec<(usize, SpecNode)> = Vec::new();
    for i in todo {
        let (r, fc) = &refs[i];
        let Ok(c_fc) = fc_from_str(fc) else { continue };
        match ied.variable_spec(r, c_fc) {
            Ok(spec) => {
                found.push((i, spec.tree()));
                members[i].spec = Some(spec);
            }
            Err(e) if e.is_connection_lost() => return Err(e),
            Err(e) => debug!(id_rtu, "anggota {r}[{fc}] tanpa titik dilewati: {e}"),
        }
    }
    let points = unmapped::virtual_points(id_rtu, found.iter().map(|(i, t)| (refs[*i].0.as_str(), refs[*i].1.as_str(), t)));
    for (i, _) in &found {
        let (r, fc) = &refs[*i];
        members[*i].unmapped = picks_for(&points, r, fc)?;
        if members[*i].unmapped.is_empty() {
            debug!(id_rtu, "anggota {r}[{fc}] tanpa titik: struktur tidak dikenal, dilewati");
        }
    }
    Ok(points)
}

/// Tulis bagian-bagian titik dari `value` ke `state`. Kembalikan indeks titik yang tersentuh.
///
/// # Safety
/// `value` harus MmsValue valid sesuai tipe anggota ini.
pub(crate) unsafe fn decode(plan: &MemberPlan, value: *mut ffi::MmsValue, points: &[Point], state: &mut [PointState], touched: &mut Vec<usize>) {
    // SAFETY: syarat pemanggil
    unsafe { decode_picks(&plan.picks, plan.spec.as_ref(), value, points, state, touched) }
}

/// Sama dengan `decode`, untuk titik virtual anggota tanpa titik (`points` = `ReportCtx::unmapped`).
///
/// # Safety
/// `value` harus MmsValue valid sesuai tipe anggota ini.
pub(crate) unsafe fn decode_unmapped(
    plan: &MemberPlan,
    value: *mut ffi::MmsValue,
    points: &[Point],
    state: &mut [PointState],
    touched: &mut Vec<usize>,
) {
    // SAFETY: syarat pemanggil
    unsafe { decode_picks(&plan.unmapped, plan.spec.as_ref(), value, points, state, touched) }
}

/// # Safety
/// `value` harus MmsValue valid sesuai `spec` (tipe anggota).
unsafe fn decode_picks(
    picks: &[Pick],
    spec: Option<&Spec>,
    value: *mut ffi::MmsValue,
    points: &[Point],
    state: &mut [PointState],
    touched: &mut Vec<usize>,
) {
    for pick in picks {
        let v = match (&pick.path, spec) {
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
    pub(crate) state: Mutex<Vec<PointState>>,
    /// EntryID report terakhir per RCB (BRCB). Hidup selama RtuCtx, jadi bertahan saat reconnect.
    // ponytail: hilang saat reload/restart FEP; simpan di DB kalau event selama restart FEP juga perlu
    entry_ids: Mutex<HashMap<String, Vec<u8>>>,
    /// IOA datapoint IEC61850 RTU ini yang tidak punya baris master_datapoint_iec61850s (diisi Manager)
    pub unmapped_ioas: Vec<i32>,
    /// Log lalu lintas data ke RTU_LOGGING (diisi Manager dari LOG_DATA_TRAFFIC / LOG_DATA_MAX_PER_SEC)
    pub traffic: TrafficLog,
    /// RTU_DATAS untuk anggota dataset RCB tanpa titik (env RTU_DATAS_UNMAPPED, diisi Manager; default true)
    pub rtu_datas_unmapped: bool,
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
            unmapped_ioas: Vec::new(),
            traffic: TrafficLog::new(false, 0),
            rtu_datas_unmapped: true,
        }
    }

    /// Baris data (REPORT/POLL/GI) ke RTU_LOGGING, kalau LOG_DATA_TRAFFIC aktif & di bawah batas.
    /// Non-blocking (atomik + BaseProducer), aman dari thread report.
    pub(crate) fn log_data(&self, line: impl FnOnce() -> String) {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
        for l in self.traffic.data_lines(now, line) {
            self.publisher.producer.send_log_raw(&self.rtu.ip_address, self.rtu.id_rtu, &l);
        }
    }

    /// Baris CONTROL: selalu dikirim (audit), tidak ikut LOG_DATA_TRAFFIC.
    pub(crate) fn log_control(&self, line: &str) {
        self.publisher.producer.send_log_raw(&self.rtu.ip_address, self.rtu.id_rtu, &format!("{} {line}", traffic::ts()));
    }

    pub(crate) fn publish(&self, i: usize, s: &PointState, action: &str, entry_time: Option<i64>, origin: Origin) {
        self.publisher.publish(&self.rtu.ip_address, &self.points[i], s, action, entry_time, origin);
    }

    /// Titik virtual anggota tanpa titik: RTU_DATAS saja (`address` null).
    pub(crate) fn publish_unmapped(&self, p: &Point, s: &PointState, action: &str, entry_time: Option<i64>, origin: Origin) {
        self.publisher.publish_unmapped(&self.rtu.ip_address, p, s, action, entry_time, origin);
    }

    fn entry_id(&self, rcb_ref: &str) -> Option<Vec<u8>> {
        self.entry_ids.lock().ok()?.get(rcb_ref).cloned()
    }

    pub(crate) fn set_entry_id(&self, rcb_ref: &str, id: Vec<u8>) {
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
    /// Browse model IED (event BROWSE). Dikerjakan bertahap (satu LN per langkah, diselingi polling).
    Browse(BrowseReply),
}

pub type BrowseReply = tokio::sync::oneshot::Sender<std::result::Result<BrowseResult, String>>;

/// Jatah waktu browse per putaran loop sesi, supaya polling / command tetap jalan selama browse.
const BROWSE_SLICE: Duration = Duration::from_millis(200);

pub(crate) enum End {
    /// Diminta berhenti lewat Ctl::Stop
    Stopped,
    /// Koneksi putus setelah sempat jalan
    Disconnected,
}

/// Peringatan konfigurasi, dikirim sekali per sesi (setelah konek): log + RTU_LOGGING.
pub fn config_warnings(ctx: &RtuCtx, use_report: bool) -> Vec<String> {
    let mut out = Vec::new();
    if use_report && ctx.rcbs.is_empty() {
        out.push(format!("mode {} tapi tidak ada RCB enabled di iec61850_report_controls", ctx.rtu.acquisition_mode));
    }
    let n = ctx.unmapped_ioas.len();
    if n > 0 {
        let ioas: Vec<String> = ctx.unmapped_ioas.iter().take(20).map(i32::to_string).collect();
        let more = if n > 20 { ", ..." } else { "" };
        out.push(format!("{n} datapoint IEC61850 tanpa baris master_datapoint_iec61850s (IOA {}{more})", ioas.join(", ")));
    }
    out
}

/// Satu sesi koneksi.
pub(crate) fn session(ctx: &Arc<RtuCtx>, rx: &Receiver<Ctl>) -> Result<End> {
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
    // max PDU hasil negosiasi initiate (MMS_MAX_PDU_SIZE = usulan FEP, IED bisa menjawab lebih kecil)
    let pdu = match crate::iec61850::client::max_pdu_from_env() {
        Some(asked) => format!("max PDU {} (diminta {asked})", ied.max_pdu_size()),
        None => format!("max PDU {}", ied.max_pdu_size()),
    };
    let msg = format!("terkoneksi MMS {}:{} mode {}, {pdu}", r.ip_address, r.port, r.acquisition_mode);
    producer.send_log(&r.ip_address, r.id_rtu, "INFO", &msg);
    info!(id_rtu = r.id_rtu, "{msg}");

    let use_report = matches!(r.acquisition_mode.as_str(), "REPORT" | "HYBRID");
    let use_poll = matches!(r.acquisition_mode.as_str(), "POLLING" | "HYBRID");
    for w in config_warnings(ctx, use_report) {
        warn!(id_rtu = r.id_rtu, "{w}");
        producer.send_log(&r.ip_address, r.id_rtu, "WARN", &w);
    }

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
            .filter(|p| polled(&r.acquisition_mode, p))
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
    // event GI: siklus polling berikutnya dikirim sebagai GENERAL_INTERROGATION
    let mut gi_poll = false;
    let mut sent: Vec<Option<PointState>> = vec![None; ctx.points.len()];
    let mut controls: HashMap<i32, Control> = HashMap::new();
    let mut browse_job: Option<(BrowseJob, BrowseReply)> = None;

    loop {
        let gi_due = last_gi.is_none_or(|t| gi_every.is_some_and(|every| t.elapsed() >= every));
        if !rcbs.is_empty() && gi_due {
            for rcb in rcbs.iter().take_while(|_| !ied.lost()) {
                send_gi(&ied, ctx, rcb);
            }
            last_gi = Some(Instant::now());
        }

        if !pending.is_empty() && !ied.lost() && Instant::now() >= next_rcb_retry {
            let recovered =
                retry_pending(&mut pending, |i| (!ied.lost()).then(|| try_setup_rcb(&ied, ctx, i, &mut report_ctxs, true)).flatten());
            // GI supaya nilai terkini langsung terkirim, tidak menunggu GI berkala.
            // last_gi None = GI awal belum jalan, nanti otomatis mencakup RCB ini.
            for rcb in recovered.iter().filter(|_| last_gi.is_some()) {
                send_gi(&ied, ctx, rcb);
            }
            rcbs.extend(recovered);
            next_rcb_retry = Instant::now() + rcb_retry;
        }

        let mut poll_timed_out = false;
        if !groups.is_empty() && Instant::now() >= next_poll {
            let is_integrity = last_integrity.is_none_or(|t| t.elapsed() >= integrity);
            if is_integrity {
                last_integrity = Some(Instant::now());
            }
            let full = full_poll_action(std::mem::take(&mut gi_poll), is_integrity);
            poll_timed_out = poll_cycle(&ied, ctx, &groups, &mut sent, full);
            let (next, skipped) = next_deadline(next_poll, Instant::now(), interval);
            if skipped > 0 {
                warn!(id_rtu = r.id_rtu, "polling terlambat, {skipped} siklus dilewati");
            }
            next_poll = next;
        }

        // Polling timeout: cek liveness SEKARANG, tidak menunggu LIVENESS_SECS. Total waktu deteksi
        // IED hang = 1 read timeout + 1 probe timeout = 2 x request_timeout_ms.
        if !ied.lost() && (poll_timed_out || ied.idle() >= liveness) {
            let reason = if poll_timed_out { "polling timeout".to_string() } else { format!("tidak ada respons IED {}s", liveness.as_secs()) };
            let result = match &probe_ref {
                Some(probe) => ied.probe(probe).map_err(|e| format!("cek {probe:?} gagal: {e}")),
                None if poll_timed_out => Err("tidak ada target cek liveness".to_string()),
                None => Ok(()),
            };
            if let Err(e) = result {
                let msg = format!("{reason}, {e}; reconnect");
                warn!(id_rtu = r.id_rtu, "{msg}");
                producer.send_log(&r.ip_address, r.id_rtu, "WARN", &msg);
                return Ok(End::Disconnected);
            }
        }

        if let Some((job, _)) = browse_job.as_mut() {
            let slice_end = Instant::now() + BROWSE_SLICE;
            let mut done = Ok(false);
            while matches!(done, Ok(false)) && Instant::now() < slice_end {
                done = job.step(&ied);
            }
            if !matches!(done, Ok(false)) {
                if let Some((job, reply)) = browse_job.take() {
                    info!(id_rtu = r.id_rtu, "browse selesai");
                    let _ = reply.send(done.map(|_| job.finish()).map_err(|e| e.to_string()));
                }
            }
        }

        if !ied.is_connected() || ied.lost() {
            return Ok(End::Disconnected);
        }
        let wait = if browse_job.is_some() {
            Duration::ZERO
        } else if groups.is_empty() {
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
                gi_poll = true;
                next_poll = Instant::now();
            }
            Ok(Ctl::Command(job)) => {
                let result = execute_command(&ied, ctx, &mut controls, &job.req);
                let msg = match &result {
                    Ok(()) => format!("{:?} ioa {} sukses", job.req.action, job.req.ioa),
                    Err(e) => format!("{:?} ioa {} gagal: {e}", job.req.action, job.req.ioa),
                };
                producer.send_log(&r.ip_address, r.id_rtu, if result.is_ok() { "INFO" } else { "WARN" }, &msg);
                let _ = job.reply.send(result.map_err(|e| e.to_string()));
            }
            Ok(Ctl::Browse(reply)) if browse_job.is_some() => {
                let _ = reply.send(Err("browse lain masih berjalan untuk IED ini".into()));
            }
            Ok(Ctl::Browse(reply)) => {
                info!(id_rtu = r.id_rtu, "browse model IED");
                match BrowseJob::start(&ied, BrowseOptions::from_env()) {
                    Ok(job) => browse_job = Some((job, reply)),
                    Err(e) => {
                        let _ = reply.send(Err(e.to_string()));
                    }
                }
            }
            Ok(Ctl::Wake) | Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

/// GI ke satu RCB + baris "GI TX".
fn send_gi(ied: &Ied, ctx: &RtuCtx, rcb: &Rcb) {
    ctx.log_data(|| traffic::gi_tx(&rcb.reference));
    if let Err(e) = rcb.gi(ied) {
        warn!(id_rtu = ctx.rtu.id_rtu, "GI {} gagal: {e}", rcb.reference);
    }
}

/// Jalankan select/operate/cancel di thread IED. Objek kontrol di-cache per IOA selama sesi,
/// supaya SELECT dan OPERATE yang datang terpisah memakai objek (ctlNum) yang sama.
/// false = tidak ada request MMS: SELECT pada ctlModel direct (operate langsung tanpa select).
fn sends_request(action: CommandAction, model: ffi::ControlModel) -> bool {
    !(action == CommandAction::Select && matches!(model, ffi::CONTROL_MODEL_DIRECT_NORMAL | ffi::CONTROL_MODEL_DIRECT_ENHANCED))
}

/// stVal (Dbpos) & q DO kontrol, untuk CONTROL_PRECHECK_POSITION. Gagal baca -> None (tidak memblok command).
fn read_position(ied: &Ied, ctx: &RtuCtx, reference: &str) -> (Option<i32>, Option<u16>) {
    let read = |attr: &str| {
        let c = CString::new(format!("{reference}.{attr}")).ok()?;
        match ied.read_object(&c, ffi::IEC61850_FC_ST) {
            Ok(v) => Some(v),
            Err(e) => {
                warn!(id_rtu = ctx.rtu.id_rtu, "precheck posisi: baca {reference}.{attr} gagal: {e}");
                None
            }
        }
    };
    (read("stVal").and_then(|v| v.dbpos()), read("q").and_then(|v| v.quality()))
}

/// Posisi tidak valid = q invalid atau stVal 00 (intermediate) / 11 (bad-state). Nilai yang tidak terbaca tidak memblok.
fn check_position(stval: Option<i32>, q: Option<u16>) -> std::result::Result<(), String> {
    let invalid = q.is_some_and(|q| q & 0b11 == ffi::QUALITY_VALIDITY_INVALID as u16);
    if !invalid && !matches!(stval, Some(0 | 3)) {
        return Ok(());
    }
    let pos = match stval {
        Some(0) => "00 (intermediate)",
        Some(1) => "01 (off)",
        Some(2) => "10 (on)",
        Some(3) => "11 (bad-state)",
        _ => "tidak terbaca",
    };
    let q = q.map_or_else(|| "tidak terbaca".to_string(), |q| super::mapper::quality_flag(Some(q), None));
    Err(format!("posisi tidak valid di IED (stVal={pos}, q={q}), SELECT tidak dikirim"))
}

/// Command ditolak FEP sebelum ada request ke IED: baris `[ERROR]` di RTU_LOGGING (bukan CONTROL TX).
fn control_rejected(ctx: &RtuCtx, action: &str, reference: &str, e: String) -> crate::error::Iec61850Error {
    let r = &ctx.rtu;
    ctx.publisher.producer.send_log(&r.ip_address, r.id_rtu, "ERROR", &traffic::control_not_sent(action, reference, &e));
    e.into()
}

fn execute_command(ied: &Ied, ctx: &RtuCtx, controls: &mut HashMap<i32, Control>, req: &CommandReq) -> Result<()> {
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
    let or_cat = p.or_cat.unwrap_or(ctx.rtu.control_or_cat);
    ctl.prepare(&req.or_ident, or_cat, p.interlock_check, p.synchro_check)?;

    let action = req.action.as_str();
    let r = &p.object_reference;
    // Validasi SEBELUM baris CONTROL TX: TX hanya ditulis kalau benar-benar ada request ke IED.
    let model = ctl.control_model();
    if model == ffi::CONTROL_MODEL_STATUS_ONLY {
        return Err(control_rejected(ctx, action, r, "ctlModel status-only, tidak bisa dikontrol".into()));
    }
    let val = match req.action {
        CommandAction::Cancel => None,
        _ => match req.value.ok_or_else(|| "value wajib diisi".to_string()).and_then(|v| ctl_value(&p.cdc, v, ctx.rtu.dpc_close_value)) {
            Ok(v) => Some(v),
            Err(e) => return Err(control_rejected(ctx, action, r, e)),
        },
    };
    if !sends_request(req.action, model) {
        ctx.log_control(&traffic::control_skip(action, r));
        return Ok(());
    }
    // CONTROL_PRECHECK_POSITION: posisi DPC tidak valid di IED -> tolak tanpa mengirim ke perangkat
    if req.action == CommandAction::Select && ctx.rtu.precheck_position && p.cdc == "DPC" {
        let (stval, q) = read_position(ied, ctx, r);
        if let Err(e) = check_position(stval, q) {
            return Err(control_rejected(ctx, action, r, e));
        }
    }
    let owned = val.map(|v| match v {
        CtlVal::Bool(b) => OwnedValue::boolean(b),
        CtlVal::Int(i) => OwnedValue::int32(i),
        CtlVal::Float(f) => OwnedValue::float(f),
        CtlVal::Tcmd(t) => OwnedValue::tcmd(t),
    });
    let ctl_text = val.map(traffic::ctl_val_text);
    if ctx.rtu.control_debug {
        let sbo = matches!(model, ffi::CONTROL_MODEL_SBO_NORMAL | ffi::CONTROL_MODEL_SBO_ENHANCED);
        let increments = match req.action {
            CommandAction::Select => true,
            CommandAction::Operate => !sbo,
            CommandAction::Cancel => false,
        };
        info!(
            id_rtu = ctx.rtu.id_rtu,
            "CONTROL DEBUG {action} {r}: ctlModel={} ctlVal={} origin{{orCat={or_cat}, orIdent={}}} ctlNum={} Check{{interlock={}, synchro={}}} Test=false",
            ctl_model_name(model as i64),
            ctl_text.as_deref().unwrap_or("(nilai SELECT terakhir)"),
            req.or_ident,
            ctl.next_ctl_num(increments),
            p.interlock_check,
            p.synchro_check,
        );
    }
    ctx.log_control(&traffic::control_tx(action, r, ctl_text.as_deref(), &req.or_ident));
    // Ok(true) = operate enhanced security: CommandTermination masih ditunggu
    let sent = match (req.action, &owned) {
        (CommandAction::Select, Some(v)) => ctl.select(v).map(|()| false),
        (CommandAction::Operate, Some(v)) => ctl.operate(v),
        (CommandAction::Cancel, _) => ctl.cancel().map(|()| false),
        (_, None) => Err("value wajib diisi".into()),
    };
    // CONTROL RX hanya untuk jawaban IED (bukan timeout / koneksi putus)
    match &sent {
        Ok(_) => ctx.log_control(&traffic::control_rx(action, r, None)),
        Err(e) if !e.is_timeout() && !ctl.connection_lost() => {
            ctx.log_control(&traffic::control_rx(action, r, Some(ctl.add_cause())));
        }
        Err(_) => {}
    }
    let result = match sent {
        Ok(true) => match ctl.wait_termination(Duration::from_millis(ctx.rtu.oper_timeout_ms)) {
            Some(positive) => {
                ctx.log_control(&traffic::termination_rx(r, positive));
                if positive { Ok(()) } else { Err(ctl.termination_error()) }
            }
            None => Err("CommandTermination tidak diterima (timeout)".into()),
        },
        Ok(false) => Ok(()),
        Err(e) => Err(e),
    };
    if ctx.rtu.control_debug && result.is_err() {
        info!(id_rtu = ctx.rtu.id_rtu, "CONTROL DEBUG {action} {r}: ctlNum={} AddCause={}", ctl.ctl_num(), traffic::add_cause_name(ctl.add_cause()));
    }
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
fn setup_rcb(ied: &Ied, ctx: &Arc<RtuCtx>, rcb_ref: &str, settings: &RcbSettings, out: &mut Vec<Box<ReportCtx>>) -> Result<Rcb> {
    let rcb = Rcb::read(ied, rcb_ref)?;
    let ds = settings.dataset.clone().or_else(|| rcb.dataset()).ok_or("RCB tidak punya dataset")?;
    let mut members = Vec::new();
    let mut refs = Vec::new();
    for m in ied.dataset_directory(&ds)? {
        let (reference, fc) = split_fc(&m).ok_or_else(|| format!("anggota tanpa FC: {m}"))?;
        members.push(build_plan(ied, &ctx.points, reference, fc)?);
        refs.push((reference.to_string(), fc.to_string()));
    }
    let mapped = members.iter().filter(|m| !m.picks.is_empty()).count();
    let todo = unmapped_candidates(ctx.rtu_datas_unmapped, &members);
    let unmapped = plan_unmapped(ied, ctx.rtu.id_rtu, &refs, &mut members, todo)?;
    let forwarded = members.iter().filter(|m| !m.unmapped.is_empty()).count();
    info!(
        id_rtu = ctx.rtu.id_rtu,
        "RCB {rcb_ref}: dataset {ds}, {} anggota, {mapped} termapping, {forwarded} diteruskan tanpa IOA",
        members.len()
    );

    let rc = Box::new(ReportCtx {
        rtu: Arc::clone(ctx),
        rcb_ref: rcb_ref.to_string(),
        dataset: ds,
        members,
        unmapped_state: Mutex::new(vec![PointState::default(); unmapped.len()]),
        unmapped,
        alive: ied.liveness(),
    });
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

/// Baca `n` grup polling berurutan. Berhenti di TIMEOUT / error koneksi pertama: IED yang hang
/// membuat setiap grup berikutnya menunggu request_timeout_ms juga (20 grup = 20x).
/// Error lain (mis. objek tidak ada) hanya melewati grup itu.
/// Kembalikan hasil yang berhasil + (indeks, error) tempat berhenti.
pub fn read_until_stuck<T>(
    n: usize,
    mut read: impl FnMut(usize) -> std::result::Result<T, ffi::IedClientError>,
) -> (Vec<(usize, T)>, Option<(usize, ffi::IedClientError)>) {
    let mut ok = Vec::new();
    for i in 0..n {
        match read(i) {
            Ok(v) => ok.push((i, v)),
            Err(e) if e == ffi::IED_ERROR_TIMEOUT || is_connection_error(e) => return (ok, Some((i, e))),
            Err(_) => {}
        }
    }
    (ok, None)
}

/// Satu siklus polling. true = ada request TIMEOUT: session() harus langsung cek liveness.
/// Titik yang dibaca lewat polling. HYBRID: status (ST) lewat report, polling hanya measurement (MX).
/// Titik CO tidak pernah dipolling.
fn polled(mode: &str, p: &Point) -> bool {
    p.fc != "CO" && (mode != "HYBRID" || p.fc == "MX")
}

/// Action siklus polling yang mengirim SEMUA titik: event GI -> GENERAL_INTERROGATION (sama dengan mode
/// REPORT), periode integrity -> INTEGRITY. None = hanya titik yang berubah (POLLING).
fn full_poll_action(gi_requested: bool, is_integrity: bool) -> Option<&'static str> {
    if gi_requested {
        Some("GENERAL_INTERROGATION")
    } else if is_integrity {
        Some("INTEGRITY")
    } else {
        None
    }
}

/// `full` = action kalau semua titik dikirim (lihat `full_poll_action`).
fn poll_cycle(ied: &Ied, ctx: &RtuCtx, groups: &[PollGroup], sent: &mut [Option<PointState>], full: Option<&'static str>) -> bool {
    let id_rtu = ctx.rtu.id_rtu;
    // Satu request MMS per (LN, FC). Dibaca dulu TANPA lock supaya callback report (mode HYBRID)
    // tidak tertahan selama request berjalan.
    let (values, stuck) = read_until_stuck(groups.len(), |i| {
        if ied.lost() {
            return Err(ffi::IED_ERROR_CONNECTION_LOST);
        }
        let g = &groups[i];
        ied.read_object(&g.reference, g.fc).map_err(|e| {
            if !e.is_timeout() {
                warn!(id_rtu, "polling {:?} gagal: {e}", g.reference);
            }
            e.code().unwrap_or(ffi::IED_ERROR_UNKNOWN)
        })
    });
    let timed_out = matches!(stuck, Some((_, e)) if e == ffi::IED_ERROR_TIMEOUT);
    if let Some((i, _)) = stuck.filter(|_| timed_out) {
        // WARN sekali per siklus, bukan per grup
        let skipped = groups.len() - i - 1;
        warn!(id_rtu, "polling {:?} timeout, {skipped} grup sisa dilewati; cek liveness sekarang", groups[i].reference);
    }

    let Ok(mut state) = ctx.state.lock() else { return timed_out };
    let mut touched = Vec::new();
    for (i, v) in &values {
        let g = &groups[*i];
        ctx.log_data(|| traffic::poll_rx(&format!("{}[{}]", g.reference.to_string_lossy(), fc_name(g.fc)), v.element_count()));
        // SAFETY: nilai hasil readObject sesuai spec LN+FC yang sama
        unsafe { decode(&g.plan, v.as_ptr(), &ctx.points, &mut state, &mut touched) };
    }
    for i in touched {
        let cur = &state[i];
        let deadband = ctx.points[i].deadband.unwrap_or(0.0);
        let action = match (full, &sent[i]) {
            (Some(a), _) => a,
            (None, None) => "POLLING",
            (None, Some(prev)) if state_changed(prev, cur, deadband) => "POLLING",
            (None, Some(_)) => continue,
        };
        ctx.publish(i, cur, action, None, Origin::Polling);
        sent[i] = Some(cur.clone());
    }
    timed_out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn precheck_posisi() {
        let invalid_failure = 2 | ffi::QUALITY_DETAIL_FAILURE as u16;
        // temuan pcap: posisi 00 + q INVALID|FAILURE -> ditolak sebelum dikirim
        let e = check_position(Some(0), Some(invalid_failure)).unwrap_err();
        assert!(e.contains("stVal=00 (intermediate)") && e.contains("INVALID|FAILURE"), "{e}");
        assert!(check_position(Some(3), Some(0)).is_err());
        assert!(check_position(Some(2), Some(2)).is_err()); // q invalid
        assert!(check_position(Some(1), Some(0)).is_ok());
        assert!(check_position(Some(2), None).is_ok());
        assert!(check_position(None, None).is_ok()); // tidak terbaca: tidak memblok
    }

    #[test]
    fn select_direct_tanpa_request() {
        use ffi::{CONTROL_MODEL_DIRECT_ENHANCED, CONTROL_MODEL_DIRECT_NORMAL, CONTROL_MODEL_SBO_ENHANCED, CONTROL_MODEL_SBO_NORMAL};
        assert!(!sends_request(CommandAction::Select, CONTROL_MODEL_DIRECT_NORMAL));
        assert!(!sends_request(CommandAction::Select, CONTROL_MODEL_DIRECT_ENHANCED));
        assert!(sends_request(CommandAction::Select, CONTROL_MODEL_SBO_NORMAL));
        assert!(sends_request(CommandAction::Select, CONTROL_MODEL_SBO_ENHANCED));
        assert!(sends_request(CommandAction::Operate, CONTROL_MODEL_DIRECT_NORMAL));
        assert!(sends_request(CommandAction::Cancel, CONTROL_MODEL_DIRECT_NORMAL));
    }

    /// (c) HYBRID: ST lewat report (source REPORT + rcb), MX lewat polling (source POLLING, rcb null).
    #[test]
    fn hybrid_st_report_mx_polling() {
        use crate::iec61850::mapping::default_value_attribute;
        let pt = |fc: &str, cdc: &str| Point {
            id_rtu: 1,
            ioa: 1,
            tag: None,
            object_reference: "LD/GGIO1.X".into(),
            fc: fc.into(),
            cdc: cdc.into(),
            value_attribute: default_value_attribute(cdc).unwrap_or_default().into(),
            ctl_model: None,
            interlock_check: false,
            synchro_check: false,
            deadband: None,
            or_cat: None,
        };
        let (st, mx, co) = (pt("ST", "SPS"), pt("MX", "MV"), pt("CO", "SPC"));
        assert!(!polled("HYBRID", &st) && polled("HYBRID", &mx) && !polled("HYBRID", &co));
        assert!(polled("POLLING", &st) && polled("POLLING", &mx) && !polled("POLLING", &co));
        // ST datang dari callback report RCB-nya, MX dari polling
        let report = Origin::Report { rcb: "LD/LLN0.BR.brcbST01" };
        assert_eq!((report.source(), report.rcb()), ("REPORT", Some("LD/LLN0.BR.brcbST01")));
        assert_eq!((Origin::Polling.source(), Origin::Polling.rcb()), ("POLLING", None));
    }

    #[test]
    fn action_polling_penuh() {
        // event GI di mode POLLING/HYBRID = GENERAL_INTERROGATION, walau bersamaan dengan periode integrity
        assert_eq!(full_poll_action(true, true), Some("GENERAL_INTERROGATION"));
        assert_eq!(full_poll_action(true, false), Some("GENERAL_INTERROGATION"));
        assert_eq!(full_poll_action(false, true), Some("INTEGRITY"));
        assert_eq!(full_poll_action(false, false), None);
    }

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
    fn ied_hang_terdeteksi_dalam_dua_kali_request_timeout() {
        let request_timeout = Duration::from_millis(100);
        let mut reads = 0;
        let t = Instant::now();
        // 20 grup polling, IED hang: setiap read menunggu request_timeout lalu TIMEOUT.
        let (ok, stuck) = read_until_stuck::<()>(20, |_| {
            reads += 1;
            std::thread::sleep(request_timeout);
            Err(ffi::IED_ERROR_TIMEOUT)
        });
        assert!(ok.is_empty());
        assert_eq!(stuck, Some((0, ffi::IED_ERROR_TIMEOUT)));
        assert_eq!(reads, 1, "grup sisa tidak boleh dibaca");
        // session() lalu langsung probe, yang juga hang 1 x request_timeout lalu gagal -> Disconnected
        std::thread::sleep(request_timeout);
        let elapsed = t.elapsed();
        assert!(elapsed < 2 * request_timeout + Duration::from_millis(80), "{elapsed:?}; tanpa perbaikan 20 x = 2 detik");
    }

    #[test]
    fn polling_error_lain_tidak_menghentikan_siklus() {
        // grup 1 tidak ada di IED (IED menjawab error) -> dilewati, grup lain tetap dibaca
        let (ok, stuck) = read_until_stuck(5, |i| if i == 1 { Err(ffi::IED_ERROR_OBJECT_DOES_NOT_EXIST) } else { Ok(i) });
        assert_eq!(ok.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![0, 2, 3, 4]);
        assert_eq!(stuck, None);
        // error koneksi -> berhenti juga (sesi diakhiri)
        let (ok, stuck) = read_until_stuck(5, |i| if i == 2 { Err(ffi::IED_ERROR_CONNECTION_LOST) } else { Ok(i) });
        assert_eq!(ok.len(), 2);
        assert_eq!(stuck, Some((2, ffi::IED_ERROR_CONNECTION_LOST)));
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
            interlock_check: false,
            synchro_check: false,
            deadband: None,
            or_cat: None,
        };
        let rcb = |r: &str| (r.to_string(), RcbSettings::default());
        assert_eq!(probe_reference(&[pt("BAY1LD0/XCBR1.Pos")], &[rcb("X/LLN0.BR.b")]).as_deref(), Some("BAY1LD0/LLN0.Mod.stVal"));
        // tanpa titik: dari RCB
        assert_eq!(probe_reference(&[], &[rcb("simpleIOGenericIO/LLN0.BR.EventsBRCB01")]).as_deref(), Some("simpleIOGenericIO/LLN0.Mod.stVal"));
        assert_eq!(probe_reference(&[pt("tanpa-slash")], &[]), None);
        assert_eq!(probe_reference(&[], &[]), None);
    }

    #[tokio::test]
    async fn peringatan_konfigurasi() {
        let mgr = crate::connection::testing::manager();
        let gate = || Arc::new(ConnectGate::new(1));
        let mut ctx = RtuCtx::new(crate::connection::testing::rtu(1), vec![], vec![], Arc::clone(&mgr.publisher), gate());
        let w = config_warnings(&ctx, true);
        assert_eq!(w.len(), 1);
        assert!(w[0].contains("tidak ada RCB enabled"));
        assert!(config_warnings(&ctx, false).is_empty()); // POLLING: RCB tidak wajib

        ctx.unmapped_ioas = (1..=25).collect();
        let w = config_warnings(&ctx, false);
        assert_eq!(w.len(), 1);
        assert!(w[0].starts_with("25 datapoint") && w[0].ends_with("20, ...)"));
    }

    #[tokio::test]
    async fn entry_id_dan_osi_di_ctx() {
        use crate::iec61850::client::{OsiSide, DEFAULT_T_SELECTOR};
        let mgr = crate::connection::testing::manager();
        let mut rtu = crate::connection::testing::rtu(1);
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

    /// Report berisi anggota termapping (XCBR1.Pos) + tanpa titik (GGIO1.Ind1): RTU_DATAS untuk keduanya (address angka
    /// vs null), DATAPOINTS hanya termapping. RTU_DATAS_UNMAPPED=false: anggota tanpa titik tidak dikirim (perilaku lama).
    #[tokio::test]
    async fn report_anggota_tanpa_titik() {
        use crate::iec61850::callbacks::report::{collect, ReportCtx};
        use crate::iec61850::client::SpecType;
        use crate::kafka::{TOPIC_DATAPOINTS, TOPIC_RTU_DATAS};

        let mgr = crate::connection::testing::manager();
        mgr.publisher.tags.add_tag(9302, 1001, "BAY1_CB_1001".into());
        let point = Point {
            id_rtu: 9302,
            ioa: 1001,
            tag: None,
            object_reference: "LD/XCBR1.Pos".into(),
            fc: "ST".into(),
            cdc: "DPS".into(),
            value_attribute: "stVal".into(),
            ctl_model: None,
            interlock_check: false,
            synchro_check: false,
            deadband: None,
            or_cat: None,
        };
        let rtu = Arc::new(RtuCtx::new(
            crate::connection::testing::rtu(9302),
            vec![],
            vec![point],
            Arc::clone(&mgr.publisher),
            Arc::new(ConnectGate::new(1)),
        ));
        let refs = [("LD/XCBR1.Pos.stVal", "ST"), ("LD/XCBR1.Pos.q", "ST"), ("LD/GGIO1.Ind1.stVal", "ST"), ("LD/GGIO1.Ind1.q", "ST")];
        let plans = || -> Vec<MemberPlan> {
            refs.iter().map(|(r, fc)| MemberPlan { spec: None, picks: picks_for(&rtu.points, r, fc).unwrap(), unmapped: vec![] }).collect()
        };
        let mut members = plans();
        assert!(unmapped_candidates(false, &members).is_empty());
        let todo = unmapped_candidates(true, &members);
        assert_eq!(todo, vec![2, 3]);
        let leaf = |name: &str, ty| SpecNode { name: name.into(), ty, children: vec![] };
        let trees = [leaf("stVal", SpecType::Bool), leaf("q", SpecType::BitString)];
        let virt = unmapped::virtual_points(9302, todo.iter().zip(&trees).map(|(&i, t)| (refs[i].0, refs[i].1, t)));
        for &i in &todo {
            members[i].unmapped = picks_for(&virt, refs[i].0, refs[i].1).unwrap();
        }
        let report_ctx = |members: Vec<MemberPlan>, unmapped: Vec<Point>| ReportCtx {
            rtu: Arc::clone(&rtu),
            rcb_ref: "LD/LLN0.BR.brcbST01".into(),
            dataset: "LD/LLN0.dsStatus".into(),
            members,
            unmapped_state: Mutex::new(vec![PointState::default(); unmapped.len()]),
            unmapped,
            alive: Arc::default(),
        };
        let ctx = report_ctx(members, virt);

        // Pos = on (Dbpos 2), Ind1 = true, q good
        let pos = OwnedValue::tcmd(2);
        let ind = OwnedValue::boolean(true);
        // SAFETY: nilai baru milik test, dihapus di akhir
        let q = unsafe { ffi::MmsValue_newBitString(13) };
        let raw = [pos.as_ptr(), q, ind.as_ptr(), q];
        let element = |i: usize| (ffi::IEC61850_REASON_DATA_CHANGE, raw[i]);

        let mut state = vec![PointState::default(); 1];
        let mut ustate = vec![PointState::default(); ctx.unmapped.len()];
        // SAFETY: raw[i] = MmsValue valid sesuai anggota ke-i
        let c = unsafe { collect(&ctx, &mut state, &mut ustate, element) };
        assert_eq!(c.mapped, vec![(0, "DATA_CHANGE")]);
        assert_eq!(c.unmapped, vec![(0, "DATA_CHANGE")]);
        assert_eq!(c.entries, 4);

        let origin = ctx.origin();
        let msgs = |p: &Point, s: &PointState, mapped: bool| -> Vec<(&'static str, Value)> {
            rtu.publisher
                .messages(&rtu.rtu.ip_address, p, mapped, s, "DATA_CHANGE", None, origin)
                .into_iter()
                .map(|(t, j)| (t, serde_json::from_str(&j).unwrap()))
                .collect()
        };
        let m = msgs(&rtu.points[0], &state[0], true);
        assert_eq!(m.iter().map(|(t, _)| *t).collect::<Vec<_>>(), vec![TOPIC_RTU_DATAS, TOPIC_DATAPOINTS]);
        let (rd, dp) = (&m[0].1, &m[1].1);
        assert_eq!((&rd["address"], &rd["value"], &rd["type"], &rd["tag"]), (&json!(1001), &json!(2), &json!("DPS"), &json!("")));
        assert_eq!((&rd["object_reference"], &rd["fc"], &rd["rcb"]), (&json!("LD/XCBR1.Pos"), &json!("ST"), &json!("LD/LLN0.BR.brcbST01")));
        // DATAPOINTS tidak berubah: tanpa object_reference / fc
        assert_eq!((&dp["tag"], &dp["address"]), (&json!("BAY1_CB_1001"), &json!(1001)));
        assert!(dp.get("object_reference").is_none() && dp.get("fc").is_none());

        let u = msgs(&ctx.unmapped[0], &ustate[0], false);
        assert_eq!(u.len(), 1, "anggota tanpa titik: RTU_DATAS saja");
        let (topic, v) = &u[0];
        assert_eq!(*topic, TOPIC_RTU_DATAS);
        assert_eq!(
            (&v["address"], &v["value"], &v["type"], &v["group"], &v["quality"], &v["tag"]),
            (&Value::Null, &json!(1), &json!("SPS"), &json!(""), &json!("GOOD"), &json!(""))
        );
        assert_eq!((&v["object_reference"], &v["fc"], &v["source"]), (&json!("LD/GGIO1.Ind1"), &json!("ST"), &json!("REPORT")));

        // RTU_DATAS_UNMAPPED=false: tidak ada titik virtual -> hanya titik termapping
        let old = report_ctx(plans(), vec![]);
        let mut state = vec![PointState::default(); 1];
        // SAFETY: idem
        let c = unsafe { collect(&old, &mut state, &mut [], element) };
        assert_eq!((c.mapped.len(), c.unmapped.len()), (1, 0));

        // SAFETY: q milik test, tidak dipakai lagi
        unsafe { ffi::MmsValue_delete(q) };
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
