//! Pembungkus aman tipis di atas libIEC61850 (awalnya dari sandbox fase 2-5, kini tools/iec61850-cli).
//!
//! Semua fungsi di sini BLOCKING (request MMS). Jangan dipanggil langsung dari task async.

use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Iec61850Error, Result};
use crate::ffi;

pub fn check(err: ffi::IedClientError) -> Result<()> {
    if err == ffi::IED_ERROR_OK {
        return Ok(());
    }
    Err(Iec61850Error::mms(err))
}

pub fn cstring(s: &str) -> Result<CString> {
    CString::new(s).map_err(|_| Iec61850Error::InvalidConfiguration(format!("string mengandung NUL: {s:?}")))
}

/// `"ST"` -> FunctionalConstraint. Error kalau FC tidak dikenal.
pub fn fc_from_str(fc: &str) -> Result<ffi::FunctionalConstraint> {
    let c = cstring(fc)?;
    // SAFETY: c hidup selama pemanggilan
    let v = unsafe { ffi::FunctionalConstraint_fromString(c.as_ptr()) };
    if v == ffi::IEC61850_FC_NONE {
        return Err(Iec61850Error::InvalidConfiguration(format!("FC tidak dikenal: {fc}")));
    }
    Ok(v)
}

pub struct ConnectParams<'a> {
    pub host: &'a str,
    pub port: i32,
    pub local_ip: Option<&'a str>,
    pub connect_timeout_ms: u32,
    pub request_timeout_ms: u32,
    /// None = parameter OSI default libIEC61850 (OSI_PARAMS_SIDE=none)
    pub osi: Option<&'a OsiParams>,
}

/// Sisi mana yang diisi ap_title/ae_qualifier/selector dari iec61850s (env OSI_PARAMS_SIDE).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OsiSide {
    /// Tidak diubah: default libIEC61850 (perilaku sebelum ini)
    #[default]
    None,
    /// Identitas FEP (calling)
    Local,
    /// Identitas IED tujuan (called)
    Remote,
}

impl OsiSide {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "none" => Some(Self::None),
            "local" => Some(Self::Local),
            "remote" => Some(Self::Remote),
            _ => None,
        }
    }
}

/// Default libIEC61850 (sama dengan default kolom di iec61850s).
pub const DEFAULT_P_SELECTOR: [u8; 4] = [0, 0, 0, 1];
pub const DEFAULT_S_SELECTOR: [u8; 2] = [0, 1];
pub const DEFAULT_T_SELECTOR: [u8; 2] = [0, 1];

/// Parameter OSI yang sudah divalidasi. `ap_title` None = AP title default libIEC61850.
#[derive(Debug, Clone, PartialEq)]
pub struct OsiParams {
    pub side: OsiSide,
    pub ap_title: Option<String>,
    pub ae_qualifier: i32,
    pub p_selector: Vec<u8>,
    pub s_selector: Vec<u8>,
    pub t_selector: Vec<u8>,
}

/// `"00 00 00 01"` -> bytes. 1..=`max` byte, tiap byte 2 digit hex dipisah spasi.
pub fn parse_selector(s: &str, max: usize) -> std::result::Result<Vec<u8>, String> {
    let bytes: Option<Vec<u8>> = s
        .split_whitespace()
        .map(|b| {
            let hex = b.len() == 2 && b.bytes().all(|c| c.is_ascii_hexdigit());
            hex.then(|| u8::from_str_radix(b, 16).ok()).flatten()
        })
        .collect();
    match bytes {
        Some(b) if !b.is_empty() && b.len() <= max => Ok(b),
        _ => Err(format!("selector {s:?} harus 1..{max} byte hex 2 digit dipisah spasi")),
    }
}

/// AP title format OID bertitik, minimal 2 komponen: `1.1.999.1`.
pub fn valid_ap_title(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    parts.len() >= 2 && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|c| c.is_ascii_digit()))
}

/// Bangun OsiParams dari kolom iec61850s. Nilai tidak valid diganti default; pesan errornya
/// dikembalikan supaya pemanggil bisa log ERROR.
pub fn osi_params(side: OsiSide, ap_title: &str, ae_qualifier: i32, p: &str, s: &str, t: &str) -> (Option<OsiParams>, Vec<String>) {
    if side == OsiSide::None {
        return (None, Vec::new());
    }
    let mut errors = Vec::new();
    let mut sel = |name: &str, v: &str, max: usize, default: &[u8]| {
        parse_selector(v, max).unwrap_or_else(|e| {
            errors.push(format!("{name}: {e}, pakai default"));
            default.to_vec()
        })
    };
    let p_selector = sel("p_selector", p, 16, &DEFAULT_P_SELECTOR[..]);
    let s_selector = sel("s_selector", s, 16, &DEFAULT_S_SELECTOR[..]);
    let t_selector = sel("t_selector", t, 4, &DEFAULT_T_SELECTOR[..]);
    let valid_ap = valid_ap_title(ap_title).then(|| ap_title.to_string());
    if valid_ap.is_none() {
        errors.push(format!("ap_title {ap_title:?} tidak valid, pakai default"));
    }
    let params = OsiParams { side, ap_title: valid_ap, ae_qualifier, p_selector, s_selector, t_selector };
    (Some(params), errors)
}

/// Bytes selector -> struct PSelector/SSelector/TSelector libIEC61850.
macro_rules! selector {
    ($ty:ty, $bytes:expr) => {{
        let b: &[u8] = $bytes;
        let mut v: $ty = <$ty>::default();
        // panjang sudah divalidasi parse_selector (<= kapasitas `value`)
        v.size = b.len() as u8;
        v.value[..b.len()].copy_from_slice(b);
        v
    }};
}

/// Tulis parameter OSI ke koneksi sebelum connect.
fn apply_osi(con: ffi::IedConnection, o: &OsiParams) -> Result<()> {
    let ap = o.ap_title.as_deref().map(cstring).transpose()?;
    let p = selector!(ffi::PSelector, &o.p_selector);
    let s = selector!(ffi::SSelector, &o.s_selector);
    let t = selector!(ffi::TSelector, &o.t_selector);
    // SAFETY: con valid & belum konek; params milik koneksi; AP title di-parse (disalin) library
    unsafe {
        let params = ffi::MmsConnection_getIsoConnectionParameters(ffi::IedConnection_getMmsConnection(con));
        if params.is_null() {
            return Err("IsoConnectionParameters tidak tersedia".into());
        }
        match o.side {
            OsiSide::Local => {
                if let Some(ap) = &ap {
                    ffi::IsoConnectionParameters_setLocalApTitle(params, ap.as_ptr(), o.ae_qualifier);
                }
                ffi::IsoConnectionParameters_setLocalAddresses(params, p, s, t);
            }
            OsiSide::Remote => {
                if let Some(ap) = &ap {
                    ffi::IsoConnectionParameters_setRemoteApTitle(params, ap.as_ptr(), o.ae_qualifier);
                }
                ffi::IsoConnectionParameters_setRemoteAddresses(params, p, s, t);
            }
            OsiSide::None => {}
        }
    }
    Ok(())
}

/// Error yang berarti koneksi MMS sudah mati: sesi harus diakhiri lalu reconnect.
pub fn is_connection_error(err: ffi::IedClientError) -> bool {
    matches!(err, ffi::IED_ERROR_CONNECTION_LOST | ffi::IED_ERROR_NOT_CONNECTED)
}

/// Error yang berarti IED MENJAWAB request (ditolak / objek tidak ada): koneksinya hidup.
pub fn is_server_response(err: ffi::IedClientError) -> bool {
    matches!(
        err,
        ffi::IED_ERROR_ACCESS_DENIED
            | ffi::IED_ERROR_OBJECT_DOES_NOT_EXIST
            | ffi::IED_ERROR_OBJECT_ACCESS_UNSUPPORTED
            | ffi::IED_ERROR_OBJECT_UNDEFINED
            | ffi::IED_ERROR_TYPE_INCONSISTENT
            | ffi::IED_ERROR_TEMPORARILY_UNAVAILABLE
    )
}

/// Kapan terakhir ada tanda hidup dari IED (request sukses / report masuk).
/// Disentuh dari thread IED dan dari thread penerima report libIEC61850.
pub struct Liveness {
    base: Instant,
    last_ms: AtomicU64,
}

impl Default for Liveness {
    fn default() -> Self {
        Self { base: Instant::now(), last_ms: AtomicU64::new(0) }
    }
}

impl Liveness {
    pub fn touch(&self) {
        self.last_ms.store(self.base.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// Lama tanpa tanda hidup.
    pub fn idle(&self) -> Duration {
        self.base.elapsed().saturating_sub(Duration::from_millis(self.last_ms.load(Ordering::Relaxed)))
    }
}

/// Koneksi MMS ke satu IED. Otomatis close + destroy saat di-drop.
pub struct Ied {
    con: ffi::IedConnection,
    /// Ada request yang gagal karena koneksi mati (getState bisa tetap CONNECTED).
    lost: Cell<bool>,
    alive: Arc<Liveness>,
}

// SAFETY: IedConnection libIEC61850 thread-safe untuk request dari satu thread pemilik + callback
// dari thread penerima internalnya. Ied hanya dipindah antar thread, tidak dipakai bersamaan.
unsafe impl Send for Ied {}

impl Ied {
    pub fn connect(p: &ConnectParams) -> Result<Self> {
        let c_host = cstring(p.host)?;
        let c_local = p.local_ip.filter(|ip| *ip != "0.0.0.0" && !ip.is_empty()).map(cstring).transpose()?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: handle baru; kalau konek gagal, Drop tetap memanggil destroy.
        let ied = Ied { con: unsafe { ffi::IedConnection_create() }, lost: Cell::new(false), alive: Arc::default() };
        // SAFETY: handle valid; setter hanya mengubah konfigurasi lokal
        unsafe {
            ffi::IedConnection_setConnectTimeout(ied.con, p.connect_timeout_ms);
            ffi::IedConnection_setRequestTimeout(ied.con, p.request_timeout_ms);
            if let Some(local) = &c_local {
                ffi::IedConnection_setLocalAddress(ied.con, local.as_ptr(), 0);
            }
        }
        if let Some(o) = p.osi {
            apply_osi(ied.con, o)?;
        }
        // SAFETY: handle valid, c_host hidup selama pemanggilan
        unsafe { ffi::IedConnection_connect(ied.con, &mut err, c_host.as_ptr(), p.port) };
        check(err)?;
        ied.alive.touch();
        Ok(ied)
    }

    /// Catat hasil satu request: sukses / dijawab IED = tanda hidup; error koneksi = sesi mati.
    fn note(&self, err: ffi::IedClientError) {
        if err == ffi::IED_ERROR_OK || is_server_response(err) {
            self.alive.touch();
        } else if is_connection_error(err) {
            self.lost.set(true);
        }
    }

    pub fn is_connected(&self) -> bool {
        // SAFETY: koneksi valid selama &self hidup
        unsafe { ffi::IedConnection_getState(self.con) == ffi::IED_STATE_CONNECTED }
    }

    /// true kalau ada request yang gagal karena koneksi mati: sesi harus diakhiri.
    pub fn lost(&self) -> bool {
        self.lost.get()
    }

    pub fn mark_lost(&self) {
        self.lost.set(true);
    }

    pub fn touch(&self) {
        self.alive.touch();
    }

    pub fn idle(&self) -> Duration {
        self.alive.idle()
    }

    /// Untuk callback report (tanda hidup dari thread penerima).
    pub fn liveness(&self) -> Arc<Liveness> {
        Arc::clone(&self.alive)
    }

    /// Cek hidup: baca `reference` [ST]. Ok juga kalau IED menjawab dengan error (mis. objek tidak
    /// ada) karena itu tetap bukti koneksinya hidup. Err = timeout / koneksi mati.
    pub fn probe(&self, reference: &CStr) -> Result<()> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, reference hidup selama pemanggilan; nilai milik kita (dibuang)
        let v = unsafe { ffi::IedConnection_readObject(self.con, &mut err, reference.as_ptr(), ffi::IEC61850_FC_ST) };
        drop(OwnedValue(v));
        self.note(err);
        if is_server_response(err) {
            return Ok(());
        }
        check(err)
    }

    /// Anggota dataset, contoh `LD/GGIO1.AnIn1.mag.f[MX]`. `ds_ref` contoh `LD/LLN0.Events`.
    pub fn dataset_directory(&self, ds_ref: &str) -> Result<Vec<String>> {
        let c = cstring(ds_ref)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan; null = tidak perlu flag deletable
        let list = unsafe {
            ffi::IedConnection_getDataSetDirectory(self.con, &mut err, c.as_ptr(), std::ptr::null_mut())
        };
        self.note(err);
        take_strings(list, err)
    }

    /// Tipe (nama child) dari sebuah reference + FC. Dipakai untuk mengambil `stVal`/`q`/`t` dari struktur.
    pub fn variable_spec(&self, reference: &str, fc: ffi::FunctionalConstraint) -> Result<Spec> {
        let c = cstring(reference)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan
        let s = unsafe { ffi::IedConnection_getVariableSpecification(self.con, &mut err, c.as_ptr(), fc) };
        let spec = Spec(s);
        self.note(err);
        check(err)?;
        if spec.0.is_null() {
            return Err(format!("spec {reference} kosong").into());
        }
        Ok(spec)
    }

    /// Baca satu objek (LN+FC, DO, atau DA) dalam satu request MMS.
    pub fn read_object(&self, reference: &CString, fc: ffi::FunctionalConstraint) -> Result<OwnedValue> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, reference hidup selama pemanggilan; nilai jadi milik kita
        let v = unsafe { ffi::IedConnection_readObject(self.con, &mut err, reference.as_ptr(), fc) };
        let v = OwnedValue(v);
        self.note(err);
        check(err)?;
        if v.0.is_null() {
            return Err("nilai kosong".into());
        }
        Ok(v)
    }

    fn raw(&self) -> ffi::IedConnection {
        self.con
    }
}

impl Drop for Ied {
    fn drop(&mut self) {
        // SAFETY: handle dari IedConnection_create, di-drop sekali. close aman walau belum konek.
        // Setelah close, tidak ada callback report lagi.
        unsafe {
            ffi::IedConnection_close(self.con);
            ffi::IedConnection_destroy(self.con);
        }
    }
}

/// MmsVariableSpecification milik kita.
pub struct Spec(*mut ffi::MmsVariableSpecification);

impl Spec {
    /// Ambil child dari `value` (yang bertipe spec ini). `path` sudah dalam format MMS (`mag$f`).
    ///
    /// # Safety
    /// `value` harus MmsValue valid yang tipenya sesuai spec ini.
    pub unsafe fn child(&self, value: *mut ffi::MmsValue, path: &CStr) -> *mut ffi::MmsValue {
        // SAFETY: syarat pemanggil; hasil dimiliki `value`
        unsafe { ffi::MmsVariableSpecification_getChildValue(self.0, value, path.as_ptr()) }
    }
}

impl Drop for Spec {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: spec dikembalikan getVariableSpecification, milik kita, dihancurkan sekali
            unsafe { ffi::MmsVariableSpecification_destroy(self.0) };
        }
    }
}

/// MmsValue milik kita (hasil readObject atau dibuat untuk ctlVal).
pub struct OwnedValue(*mut ffi::MmsValue);

impl OwnedValue {
    pub fn boolean(v: bool) -> Self {
        // SAFETY: membuat nilai baru, jadi milik kita
        Self(unsafe { ffi::MmsValue_newBoolean(v) })
    }

    pub fn int32(v: i32) -> Self {
        // SAFETY: membuat nilai baru, jadi milik kita
        Self(unsafe { ffi::MmsValue_newIntegerFromInt32(v) })
    }

    pub fn octet_string(b: &[u8]) -> Self {
        let n = b.len() as i32;
        // SAFETY: nilai baru milik kita; buffer b hanya dibaca (disalin) selama pemanggilan
        unsafe {
            let v = ffi::MmsValue_newOctetString(n, n);
            if !v.is_null() {
                ffi::MmsValue_setOctetString(v, b.as_ptr(), n);
            }
            Self(v)
        }
    }

    pub fn as_ptr(&self) -> *mut ffi::MmsValue {
        self.0
    }
}

/// Isi octet string (mis. EntryID report). None kalau null / bukan octet string.
///
/// # Safety
/// `v` harus null atau MmsValue valid selama pemanggilan.
pub unsafe fn octet_bytes(v: *mut ffi::MmsValue) -> Option<Vec<u8>> {
    // SAFETY: syarat pemanggil; buffer milik v dan langsung disalin
    unsafe {
        if v.is_null() || ffi::MmsValue_getType(v) != ffi::MMS_OCTET_STRING {
            return None;
        }
        let n = ffi::MmsValue_getOctetStringSize(v) as usize;
        let p = ffi::MmsValue_getOctetStringBuffer(v);
        (!p.is_null()).then(|| std::slice::from_raw_parts(p, n).to_vec())
    }
}

impl Drop for OwnedValue {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: nilai milik kita dari readObject, dihapus sekali
            unsafe { ffi::MmsValue_delete(self.0) };
        }
    }
}

/// Satu Report Control Block (BRCB `.BR.` / URCB `.RP.`).
pub struct Rcb {
    pub reference: String,
    c_ref: CString,
    rcb: ffi::ClientReportControlBlock,
}

/// Parameter RCB dari DB. `None` = TIDAK ditulis ke IED (pakai setting IED).
#[derive(Debug, Clone, Default)]
pub struct RcbSettings {
    pub dataset: Option<String>,
    pub trg_ops: Option<i32>,
    pub intg_pd_ms: Option<u32>,
    pub buf_tm_ms: Option<u32>,
}

/// Nilai RCB di IED saat ini (hasil getRCBValues) yang relevan untuk enable.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RcbCurrent {
    pub buffered: bool,
    pub dataset: Option<String>,
    pub trg_ops: i32,
    pub intg_pd_ms: u32,
    pub buf_tm_ms: u32,
    pub opt_flds: i32,
    pub has_resv_tms: bool,
    pub resv_tms: i16,
}

/// Elemen yang akan ditulis saat enable. None = tidak ditulis.
#[derive(Debug, Default, PartialEq)]
pub struct EnablePlan {
    pub dataset: Option<String>,
    pub trg_ops: Option<i32>,
    pub intg_pd_ms: Option<u32>,
    pub buf_tm_ms: Option<u32>,
    pub opt_flds: Option<i32>,
    pub resv_tms: Option<i16>,
}

fn changed<T: PartialEq>(want: Option<T>, cur: T) -> Option<T> {
    want.filter(|w| *w != cur)
}

/// Hanya tulis yang BERBEDA dari IED: menulis ulang DatSet dkk. ke BRCB bisa mengosongkan
/// buffer IED, padahal event selama putus justru mau diambil lewat EntryID.
/// OptFlds selalu memuat reason (action), time-stamp (waktu entri), dan entryID (BRCB).
pub fn plan_enable(cur: &RcbCurrent, want: &RcbSettings, resv_tms_secs: i16) -> EnablePlan {
    let mut need = ffi::RPT_OPT_REASON_FOR_INCLUSION | ffi::RPT_OPT_TIME_STAMP;
    if cur.buffered {
        need |= ffi::RPT_OPT_ENTRY_ID;
    }
    let opt = cur.opt_flds | need as i32;
    // ResvTms < 0 = reservasi dari konfigurasi IED, jangan ditimpa
    let resv = cur.buffered && cur.has_resv_tms && cur.resv_tms >= 0 && resv_tms_secs > 0;
    EnablePlan {
        dataset: want.dataset.clone().filter(|d| cur.dataset.as_deref() != Some(d.as_str())),
        trg_ops: changed(want.trg_ops, cur.trg_ops),
        intg_pd_ms: changed(want.intg_pd_ms, cur.intg_pd_ms),
        buf_tm_ms: changed(want.buf_tm_ms, cur.buf_tm_ms),
        opt_flds: (opt != cur.opt_flds).then_some(opt),
        resv_tms: changed(resv.then_some(resv_tms_secs), cur.resv_tms),
    }
}

impl Rcb {
    pub fn read(ied: &Ied, reference: &str) -> Result<Self> {
        let c_ref = cstring(reference)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c_ref hidup; null = buat objek RCB baru
        let rcb = unsafe {
            ffi::IedConnection_getRCBValues(ied.raw(), &mut err, c_ref.as_ptr(), std::ptr::null_mut())
        };
        let me = Self { reference: reference.into(), c_ref, rcb };
        ied.note(err);
        check(err)?;
        if me.rcb.is_null() {
            return Err("RCB tidak ditemukan".into());
        }
        Ok(me)
    }

    pub fn is_buffered(&self) -> bool {
        // SAFETY: rcb valid (dicek di read)
        unsafe { ffi::ClientReportControlBlock_isBuffered(self.rcb) }
    }

    /// Dataset yang terpasang di RCB, format ACSI (`LD/LLN0.Events`).
    pub fn dataset(&self) -> Option<String> {
        // SAFETY: string milik rcb, langsung disalin
        let p = unsafe { ffi::ClientReportControlBlock_getDataSetReference(self.rcb) };
        if p.is_null() {
            return None;
        }
        let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().replace('$', ".");
        (!s.is_empty()).then_some(s)
    }

    /// # Safety
    /// `param` harus valid selama koneksi `ied` hidup (callback dari thread libIEC61850).
    pub unsafe fn install_handler(&self, ied: &Ied, handler: ffi::ReportCallbackFunction, param: *mut c_void) {
        // SAFETY: rptId milik rcb dan disalin oleh library; syarat param dijamin pemanggil
        unsafe {
            let rpt_id = ffi::ClientReportControlBlock_getRptId(self.rcb);
            ffi::IedConnection_installReportHandler(ied.raw(), self.c_ref.as_ptr(), rpt_id, handler, param);
        }
    }

    /// Lepas handler report. Setelah kembali, libIEC61850 tidak akan memanggil callback
    /// dengan `param` lama lagi (pemanggilan callback & uninstall memakai mutex yang sama).
    pub fn uninstall_handler(&self, ied: &Ied) {
        // SAFETY: koneksi dan c_ref valid
        unsafe { ffi::IedConnection_uninstallReportHandler(ied.raw(), self.c_ref.as_ptr()) };
    }

    /// Nilai RCB yang dibaca saat `read`.
    pub fn current(&self) -> RcbCurrent {
        // SAFETY: rcb valid; getter hanya membaca objek lokal
        unsafe {
            RcbCurrent {
                buffered: self.is_buffered(),
                dataset: self.dataset(),
                trg_ops: ffi::ClientReportControlBlock_getTrgOps(self.rcb),
                intg_pd_ms: ffi::ClientReportControlBlock_getIntgPd(self.rcb),
                buf_tm_ms: ffi::ClientReportControlBlock_getBufTm(self.rcb),
                opt_flds: ffi::ClientReportControlBlock_getOptFlds(self.rcb),
                has_resv_tms: ffi::ClientReportControlBlock_hasResvTms(self.rcb),
                resv_tms: ffi::ClientReportControlBlock_getResvTms(self.rcb),
            }
        }
    }

    /// Tulis EntryID (BRCB, sebelum enable): IED mengirim entri buffer SETELAH entri ini.
    pub fn write_entry_id(&self, ied: &Ied, entry_id: &[u8]) -> Result<()> {
        let v = OwnedValue::octet_string(entry_id);
        if v.as_ptr().is_null() {
            return Err("gagal membuat nilai EntryID".into());
        }
        // SAFETY: rcb & v valid; library menyalin (clone) EntryID, v tetap milik kita
        unsafe { ffi::ClientReportControlBlock_setEntryId(self.rcb, v.as_ptr()) };
        self.write(ied, ffi::RCB_ELEMENT_ENTRY_ID)
    }

    pub fn enable(&self, ied: &Ied, s: &RcbSettings, resv_tms_secs: i16) -> Result<()> {
        let plan = plan_enable(&self.current(), s, resv_tms_secs);
        let mut mask = ffi::RCB_ELEMENT_RPT_ENA;
        // MMS memakai `$` antara LN dan nama dataset: LD/LLN0$Events
        let ds = plan.dataset.as_deref().map(|d| cstring(&d.replacen('.', "$", 1))).transpose()?;
        // SAFETY: rcb valid; setter hanya mengubah objek lokal sebelum ditulis
        unsafe {
            if let Some(ds) = &ds {
                ffi::ClientReportControlBlock_setDataSetReference(self.rcb, ds.as_ptr());
                mask |= ffi::RCB_ELEMENT_DATSET;
            }
            if let Some(t) = plan.trg_ops {
                ffi::ClientReportControlBlock_setTrgOps(self.rcb, t);
                mask |= ffi::RCB_ELEMENT_TRG_OPS;
            }
            if let Some(p) = plan.intg_pd_ms {
                ffi::ClientReportControlBlock_setIntgPd(self.rcb, p);
                mask |= ffi::RCB_ELEMENT_INTG_PD;
            }
            if let Some(b) = plan.buf_tm_ms {
                ffi::ClientReportControlBlock_setBufTm(self.rcb, b);
                mask |= ffi::RCB_ELEMENT_BUF_TM;
            }
            if let Some(o) = plan.opt_flds {
                ffi::ClientReportControlBlock_setOptFlds(self.rcb, o);
                mask |= ffi::RCB_ELEMENT_OPT_FLDS;
            }
            if let Some(r) = plan.resv_tms {
                // BRCB tetap dipesan untuk FEP selama r detik setelah koneksi putus
                ffi::ClientReportControlBlock_setResvTms(self.rcb, r);
                mask |= ffi::RCB_ELEMENT_RESV_TMS;
            }
            if !self.is_buffered() {
                ffi::ClientReportControlBlock_setResv(self.rcb, true);
                mask |= ffi::RCB_ELEMENT_RESV;
            }
            ffi::ClientReportControlBlock_setRptEna(self.rcb, true);
        }
        self.write(ied, mask)
    }

    pub fn gi(&self, ied: &Ied) -> Result<()> {
        // SAFETY: rcb valid
        unsafe { ffi::ClientReportControlBlock_setGI(self.rcb, true) };
        self.write(ied, ffi::RCB_ELEMENT_GI)
    }

    fn write(&self, ied: &Ied, mask: u32) -> Result<()> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi dan rcb valid; true = satu request
        unsafe { ffi::IedConnection_setRCBValues(ied.raw(), &mut err, self.rcb, mask, true) };
        ied.note(err);
        check(err)
    }
}

impl Drop for Rcb {
    fn drop(&mut self) {
        if !self.rcb.is_null() {
            // SAFETY: rcb milik kita, dihancurkan sekali
            unsafe { ffi::ClientReportControlBlock_destroy(self.rcb) };
        }
    }
}

/// Hasil CommandTermination (enhanced security). Diisi dari thread penerima libIEC61850.
#[derive(Default)]
struct Termination {
    result: Mutex<Option<bool>>,
    cv: Condvar,
}

/// Objek kontrol (ControlObjectClient) untuk satu DO kontrol, mis. `LD/CSWI1.Pos`.
pub struct Control {
    ctl: ffi::ControlObjectClient,
    // Box: alamat harus tetap, dipegang callback C
    termination: Box<Termination>,
}

unsafe extern "C" fn on_termination(param: *mut c_void, ctl: ffi::ControlObjectClient) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: param = &Termination milik Control yang masih hidup (handler dilepas saat Drop
        // karena ControlObjectClient_destroy).
        let t = unsafe { &*(param as *const Termination) };
        let ok = unsafe { ffi::ControlObjectClient_getLastApplError(ctl) }.error == ffi::CONTROL_ERROR_NO_ERROR;
        if let Ok(mut r) = t.result.lock() {
            *r = Some(ok);
            t.cv.notify_all();
        }
    }));
}

impl Control {
    /// Membuat objek kontrol. Membaca ctlModel & tipe ctlVal dari IED (request MMS).
    pub fn create(ied: &Ied, reference: &str) -> Result<Self> {
        let c = cstring(reference)?;
        // SAFETY: koneksi valid, c hidup selama pemanggilan
        let ctl = unsafe { ffi::ControlObjectClient_create(c.as_ptr(), ied.raw()) };
        if ctl.is_null() {
            return Err(format!("objek kontrol {reference} tidak ditemukan / tidak bisa dibaca").into());
        }
        let me = Self { ctl, termination: Box::default() };
        let param = &*me.termination as *const Termination as *mut c_void;
        // SAFETY: Termination di heap, hidup selama Control (handler ikut hilang saat destroy)
        unsafe { ffi::ControlObjectClient_setCommandTerminationHandler(me.ctl, Some(on_termination), param) };
        Ok(me)
    }

    pub fn control_model(&self) -> ffi::ControlModel {
        // SAFETY: ctl valid
        unsafe { ffi::ControlObjectClient_getControlModel(self.ctl) }
    }

    /// Timpa ctlModel hasil baca IED (dari kolom ctl_model di DB).
    pub fn set_control_model(&self, m: ffi::ControlModel) {
        // SAFETY: ctl valid; hanya mengubah objek lokal
        unsafe { ffi::ControlObjectClient_setControlModel(self.ctl, m) };
    }

    /// orIdent + orCat (remote-control), interlock & synchro check.
    pub fn prepare(&self, or_ident: &str, interlock: bool, synchro: bool) -> Result<()> {
        let ident = cstring(or_ident)?;
        // SAFETY: ctl valid; orIdent disalin library
        unsafe {
            ffi::ControlObjectClient_setOrigin(self.ctl, ident.as_ptr(), ffi::CONTROL_ORCAT_REMOTE_CONTROL as i32);
            ffi::ControlObjectClient_setInterlockCheck(self.ctl, interlock);
            ffi::ControlObjectClient_setSynchroCheck(self.ctl, synchro);
        }
        Ok(())
    }

    /// SBO normal: select; SBO enhanced: select-with-value; model direct: tidak ada yang perlu dipilih.
    pub fn select(&self, value: &OwnedValue) -> Result<()> {
        // SAFETY: ctl & value valid; value tidak diambil alih library
        let ok = match self.control_model() {
            ffi::CONTROL_MODEL_SBO_NORMAL => unsafe { ffi::ControlObjectClient_select(self.ctl) },
            ffi::CONTROL_MODEL_SBO_ENHANCED => unsafe { ffi::ControlObjectClient_selectWithValue(self.ctl, value.as_ptr()) },
            ffi::CONTROL_MODEL_STATUS_ONLY => return Err("ctlModel status-only, tidak bisa dikontrol".into()),
            _ => return Ok(()),
        };
        if ok { Ok(()) } else { Err(self.error("select")) }
    }

    /// Operate. Enhanced security: tunggu CommandTermination (maks `wait`).
    pub fn operate(&self, value: &OwnedValue, wait: std::time::Duration) -> Result<()> {
        if let Ok(mut r) = self.termination.result.lock() {
            *r = None;
        }
        // SAFETY: ctl & value valid; operTime 0 = langsung
        if !unsafe { ffi::ControlObjectClient_operate(self.ctl, value.as_ptr(), 0) } {
            return Err(self.error("operate"));
        }
        if !matches!(self.control_model(), ffi::CONTROL_MODEL_DIRECT_ENHANCED | ffi::CONTROL_MODEL_SBO_ENHANCED) {
            return Ok(());
        }
        let guard = self.termination.result.lock().map_err(|_| "lock termination rusak")?;
        let (guard, _) = self
            .termination
            .cv
            .wait_timeout_while(guard, wait, |r| r.is_none())
            .map_err(|_| "lock termination rusak")?;
        match *guard {
            Some(true) => Ok(()),
            Some(false) => Err(self.error("CommandTermination-")),
            None => Err("CommandTermination tidak diterima (timeout)".into()),
        }
    }

    pub fn cancel(&self) -> Result<()> {
        // SAFETY: ctl valid
        if unsafe { ffi::ControlObjectClient_cancel(self.ctl) } {
            Ok(())
        } else {
            Err(self.error("cancel"))
        }
    }

    /// true kalau perintah terakhir gagal karena koneksi mati.
    pub fn connection_lost(&self) -> bool {
        // SAFETY: ctl valid
        is_connection_error(unsafe { ffi::ControlObjectClient_getLastError(self.ctl) })
    }

    /// Error perintah terakhir. Ada kode IedClientError -> `Mms` (bisa dicek timeout/putus);
    /// ditolak IED tanpa kode (LastApplError saja) -> `Other`.
    fn error(&self, what: &str) -> Iec61850Error {
        // SAFETY: ctl valid
        let (err, appl) = unsafe {
            (ffi::ControlObjectClient_getLastError(self.ctl), ffi::ControlObjectClient_getLastApplError(self.ctl))
        };
        let base = check(err).err().map_or_else(|| "ditolak IED".to_string(), |e| e.to_string());
        let msg = format!("{what} gagal: {base} (LastApplError error={}, addCause={})", appl.error, appl.addCause);
        if err == ffi::IED_ERROR_OK {
            Iec61850Error::Other(msg)
        } else {
            Iec61850Error::Mms { code: err, msg }
        }
    }
}

impl Drop for Control {
    fn drop(&mut self) {
        // SAFETY: ctl milik kita, dihancurkan sekali
        unsafe { ffi::ControlObjectClient_destroy(self.ctl) };
    }
}

/// Teks ctl_model di DB -> ControlModel.
pub fn control_model_from_str(s: &str) -> Option<ffi::ControlModel> {
    Some(match s {
        "status-only" => ffi::CONTROL_MODEL_STATUS_ONLY,
        "direct-with-normal-security" => ffi::CONTROL_MODEL_DIRECT_NORMAL,
        "sbo-with-normal-security" => ffi::CONTROL_MODEL_SBO_NORMAL,
        "direct-with-enhanced-security" => ffi::CONTROL_MODEL_DIRECT_ENHANCED,
        "sbo-with-enhanced-security" => ffi::CONTROL_MODEL_SBO_ENHANCED,
        _ => return None,
    })
}

/// Salin LinkedList<char*> ke Vec<String>, lalu bebaskan list-nya.
fn take_strings(list: ffi::LinkedList, err: ffi::IedClientError) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if !list.is_null() {
        // SAFETY: list milik kita; elemen pertama kepala kosong; data = char* malloc.
        // LinkedList_destroy membebaskan list + data.
        unsafe {
            let mut e = ffi::LinkedList_getNext(list);
            while !e.is_null() {
                let data = ffi::LinkedList_getData(e) as *const std::os::raw::c_char;
                if !data.is_null() {
                    out.push(CStr::from_ptr(data).to_string_lossy().into_owned());
                }
                e = ffi::LinkedList_getNext(e);
            }
            ffi::LinkedList_destroy(list);
        }
    }
    check(err).map(|_| out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn klasifikasi_error() {
        assert!(is_connection_error(ffi::IED_ERROR_CONNECTION_LOST));
        assert!(is_connection_error(ffi::IED_ERROR_NOT_CONNECTED));
        // timeout bukan bukti putus (bisa IED lambat) -> ditangani liveness
        assert!(!is_connection_error(ffi::IED_ERROR_TIMEOUT));
        assert!(!is_connection_error(ffi::IED_ERROR_OBJECT_DOES_NOT_EXIST));
        // IED menjawab -> koneksi hidup
        assert!(is_server_response(ffi::IED_ERROR_OBJECT_DOES_NOT_EXIST));
        assert!(is_server_response(ffi::IED_ERROR_ACCESS_DENIED));
        assert!(!is_server_response(ffi::IED_ERROR_TIMEOUT));
        assert!(!is_server_response(ffi::IED_ERROR_CONNECTION_LOST));
    }

    #[test]
    fn liveness_idle_dan_touch() {
        let l = Liveness::default();
        std::thread::sleep(Duration::from_millis(60));
        assert!(l.idle() >= Duration::from_millis(50));
        l.touch();
        assert!(l.idle() < Duration::from_millis(50));
        // disentuh dari thread lain (callback report)
        let l = Arc::new(l);
        std::thread::sleep(Duration::from_millis(60));
        let l2 = Arc::clone(&l);
        std::thread::spawn(move || l2.touch()).join().unwrap();
        assert!(l.idle() < Duration::from_millis(50));
    }

    #[test]
    fn selector_hex() {
        assert_eq!(parse_selector("00 00 00 01", 16), Ok(vec![0, 0, 0, 1]));
        assert_eq!(parse_selector("aB ff", 4), Ok(vec![0xab, 0xff]));
        assert!(parse_selector("", 4).is_err());
        assert!(parse_selector("0001", 4).is_err()); // tanpa spasi
        assert!(parse_selector("0g 01", 4).is_err());
        assert!(parse_selector("+1 01", 4).is_err());
        assert!(parse_selector("00 01 02 03 04", 4).is_err()); // T-selector maks 4 byte
    }

    #[test]
    fn ap_title() {
        assert!(valid_ap_title("1.1.999.1"));
        assert!(!valid_ap_title("1"));
        assert!(!valid_ap_title("1..2"));
        assert!(!valid_ap_title("1.a.2"));
    }

    #[test]
    fn sisi_osi() {
        assert_eq!(OsiSide::parse(""), Some(OsiSide::None));
        assert_eq!(OsiSide::parse("Remote"), Some(OsiSide::Remote));
        assert_eq!(OsiSide::parse(" local "), Some(OsiSide::Local));
        assert_eq!(OsiSide::parse("both"), None);
    }

    #[test]
    fn parameter_osi() {
        // none = perilaku lama, kolom tidak dibaca sama sekali
        assert_eq!(osi_params(OsiSide::None, "x", 1, "x", "x", "x"), (None, vec![]));

        let (p, err) = osi_params(OsiSide::Remote, "1.1.999.1", 12, "00 00 00 02", "00 01", "00 01");
        assert!(err.is_empty());
        let p = p.unwrap();
        assert_eq!((p.ap_title.as_deref(), p.ae_qualifier, p.p_selector), (Some("1.1.999.1"), 12, vec![0, 0, 0, 2]));

        // selector & ap_title rusak -> default + pesan error (untuk log ERROR)
        let (p, err) = osi_params(OsiSide::Local, "abc", 12, "zz", "00 01", "00 01 02 03 04");
        let p = p.unwrap();
        assert_eq!(err.len(), 3);
        assert_eq!(p.ap_title, None);
        assert_eq!(p.p_selector, DEFAULT_P_SELECTOR.to_vec());
        assert_eq!(p.t_selector, DEFAULT_T_SELECTOR.to_vec());
    }

    #[test]
    fn struct_selector() {
        let t = selector!(ffi::TSelector, &[0, 1]);
        assert_eq!((t.size, t.value), (2, [0, 1, 0, 0]));
    }

    #[test]
    fn entry_id_bolak_balik() {
        let id = [1u8, 2, 3, 4, 5, 6, 7, 0xff];
        let v = OwnedValue::octet_string(&id);
        // SAFETY: v valid milik test
        assert_eq!(unsafe { octet_bytes(v.as_ptr()) }, Some(id.to_vec()));
        let b = OwnedValue::boolean(true);
        assert_eq!(unsafe { octet_bytes(b.as_ptr()) }, None);
    }

    fn opt_wajib(buffered: bool) -> i32 {
        let mut o = ffi::RPT_OPT_REASON_FOR_INCLUSION | ffi::RPT_OPT_TIME_STAMP;
        if buffered {
            o |= ffi::RPT_OPT_ENTRY_ID;
        }
        o as i32
    }

    #[test]
    fn rencana_enable_hanya_yang_berubah() {
        let cur = RcbCurrent {
            buffered: true,
            dataset: Some("LD/LLN0.Events".into()),
            trg_ops: 27,
            intg_pd_ms: 5000,
            buf_tm_ms: 100,
            opt_flds: opt_wajib(true),
            has_resv_tms: true,
            resv_tms: 60,
        };
        // Semua sudah sama dengan IED -> hanya RptEna yang ditulis (buffer BRCB aman)
        let same = RcbSettings { dataset: Some("LD/LLN0.Events".into()), trg_ops: Some(27), intg_pd_ms: Some(5000), buf_tm_ms: None };
        assert_eq!(plan_enable(&cur, &same, 60), EnablePlan::default());

        let want = RcbSettings { dataset: Some("LD/LLN0.Other".into()), trg_ops: Some(19), intg_pd_ms: None, buf_tm_ms: Some(200) };
        let plan = plan_enable(&RcbCurrent { opt_flds: 0, resv_tms: 0, ..cur.clone() }, &want, 60);
        assert_eq!(plan.dataset.as_deref(), Some("LD/LLN0.Other"));
        assert_eq!((plan.trg_ops, plan.intg_pd_ms, plan.buf_tm_ms), (Some(19), None, Some(200)));
        assert_eq!(plan.opt_flds, Some(opt_wajib(true)));
        assert_eq!(plan.resv_tms, Some(60));

        // ResvTms tidak ada di IED / -1 (dari konfigurasi) / env 0 -> tidak ditulis
        assert_eq!(plan_enable(&RcbCurrent { has_resv_tms: false, resv_tms: 0, ..cur.clone() }, &same, 60).resv_tms, None);
        assert_eq!(plan_enable(&RcbCurrent { resv_tms: -1, ..cur.clone() }, &same, 60).resv_tms, None);
        assert_eq!(plan_enable(&RcbCurrent { resv_tms: 0, ..cur.clone() }, &same, 0).resv_tms, None);

        // URCB: tanpa entryID & ResvTms
        let urcb = RcbCurrent { buffered: false, opt_flds: 0, ..cur };
        let plan = plan_enable(&urcb, &same, 60);
        assert_eq!((plan.opt_flds, plan.resv_tms), (Some(opt_wajib(false)), None));
    }
}
