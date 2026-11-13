//! Pembungkus aman tipis di atas libIEC61850 (berasal dari iec61850-hello fase 2-5).
//!
//! Semua fungsi di sini BLOCKING (request MMS). Jangan dipanggil langsung dari task async.

use std::ffi::{CStr, CString};
use std::os::raw::c_void;

use crate::ffi;

pub fn check(err: ffi::IedClientError) -> Result<(), String> {
    if err == ffi::IED_ERROR_OK {
        return Ok(());
    }
    // SAFETY: IedClientError_toString mengembalikan string statis milik library
    Err(unsafe { CStr::from_ptr(ffi::IedClientError_toString(err)) }
        .to_string_lossy()
        .into_owned())
}

pub fn cstring(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| format!("string mengandung NUL: {s:?}"))
}

/// `"ST"` -> FunctionalConstraint. Error kalau FC tidak dikenal.
pub fn fc_from_str(fc: &str) -> Result<ffi::FunctionalConstraint, String> {
    let c = cstring(fc)?;
    // SAFETY: c hidup selama pemanggilan
    let v = unsafe { ffi::FunctionalConstraint_fromString(c.as_ptr()) };
    if v == ffi::IEC61850_FC_NONE {
        return Err(format!("FC tidak dikenal: {fc}"));
    }
    Ok(v)
}

pub struct ConnectParams<'a> {
    pub host: &'a str,
    pub port: i32,
    pub local_ip: Option<&'a str>,
    pub connect_timeout_ms: u32,
    pub request_timeout_ms: u32,
}

/// Koneksi MMS ke satu IED. Otomatis close + destroy saat di-drop.
pub struct Ied(ffi::IedConnection);

// SAFETY: IedConnection libIEC61850 thread-safe untuk request dari satu thread pemilik + callback
// dari thread penerima internalnya. Ied hanya dipindah antar thread, tidak dipakai bersamaan.
unsafe impl Send for Ied {}

impl Ied {
    pub fn connect(p: &ConnectParams) -> Result<Self, String> {
        let c_host = cstring(p.host)?;
        let c_local = p.local_ip.filter(|ip| *ip != "0.0.0.0" && !ip.is_empty()).map(cstring).transpose()?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: handle baru; kalau konek gagal, Drop tetap memanggil destroy.
        let ied = Ied(unsafe { ffi::IedConnection_create() });
        unsafe {
            ffi::IedConnection_setConnectTimeout(ied.0, p.connect_timeout_ms);
            ffi::IedConnection_setRequestTimeout(ied.0, p.request_timeout_ms);
            if let Some(local) = &c_local {
                ffi::IedConnection_setLocalAddress(ied.0, local.as_ptr(), 0);
            }
            ffi::IedConnection_connect(ied.0, &mut err, c_host.as_ptr(), p.port);
        }
        check(err).map(|_| ied)
    }

    pub fn is_connected(&self) -> bool {
        // SAFETY: koneksi valid selama &self hidup
        unsafe { ffi::IedConnection_getState(self.0) == ffi::IED_STATE_CONNECTED }
    }

    /// Anggota dataset, contoh `LD/GGIO1.AnIn1.mag.f[MX]`. `ds_ref` contoh `LD/LLN0.Events`.
    pub fn dataset_directory(&self, ds_ref: &str) -> Result<Vec<String>, String> {
        let c = cstring(ds_ref)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan; null = tidak perlu flag deletable
        let list = unsafe {
            ffi::IedConnection_getDataSetDirectory(self.0, &mut err, c.as_ptr(), std::ptr::null_mut())
        };
        take_strings(list, err)
    }

    /// Tipe (nama child) dari sebuah reference + FC. Dipakai untuk mengambil `stVal`/`q`/`t` dari struktur.
    pub fn variable_spec(&self, reference: &str, fc: ffi::FunctionalConstraint) -> Result<Spec, String> {
        let c = cstring(reference)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan
        let s = unsafe { ffi::IedConnection_getVariableSpecification(self.0, &mut err, c.as_ptr(), fc) };
        let spec = Spec(s);
        check(err)?;
        if spec.0.is_null() {
            return Err(format!("spec {reference} kosong"));
        }
        Ok(spec)
    }

    /// Baca satu objek (LN+FC, DO, atau DA) dalam satu request MMS.
    pub fn read_object(&self, reference: &CString, fc: ffi::FunctionalConstraint) -> Result<OwnedValue, String> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, reference hidup selama pemanggilan; nilai jadi milik kita
        let v = unsafe { ffi::IedConnection_readObject(self.0, &mut err, reference.as_ptr(), fc) };
        let v = OwnedValue(v);
        check(err)?;
        if v.0.is_null() {
            return Err("nilai kosong".into());
        }
        Ok(v)
    }

    fn raw(&self) -> ffi::IedConnection {
        self.0
    }
}

impl Drop for Ied {
    fn drop(&mut self) {
        // SAFETY: handle dari IedConnection_create, di-drop sekali. close aman walau belum konek.
        // Setelah close, tidak ada callback report lagi.
        unsafe {
            ffi::IedConnection_close(self.0);
            ffi::IedConnection_destroy(self.0);
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

/// MmsValue hasil readObject (milik kita).
pub struct OwnedValue(*mut ffi::MmsValue);

impl OwnedValue {
    pub fn as_ptr(&self) -> *mut ffi::MmsValue {
        self.0
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

impl Rcb {
    pub fn read(ied: &Ied, reference: &str) -> Result<Self, String> {
        let c_ref = cstring(reference)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c_ref hidup; null = buat objek RCB baru
        let rcb = unsafe {
            ffi::IedConnection_getRCBValues(ied.raw(), &mut err, c_ref.as_ptr(), std::ptr::null_mut())
        };
        let me = Self { reference: reference.into(), c_ref, rcb };
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

    pub fn enable(&self, ied: &Ied, s: &RcbSettings) -> Result<(), String> {
        let mut mask = ffi::RCB_ELEMENT_RPT_ENA;
        // MMS memakai `$` antara LN dan nama dataset: LD/LLN0$Events
        let ds = s.dataset.as_deref().map(|d| cstring(&d.replacen('.', "$", 1))).transpose()?;
        // SAFETY: rcb valid; setter hanya mengubah objek lokal sebelum ditulis
        unsafe {
            if let Some(ds) = &ds {
                ffi::ClientReportControlBlock_setDataSetReference(self.rcb, ds.as_ptr());
                mask |= ffi::RCB_ELEMENT_DATSET;
            }
            if let Some(t) = s.trg_ops {
                ffi::ClientReportControlBlock_setTrgOps(self.rcb, t);
                mask |= ffi::RCB_ELEMENT_TRG_OPS;
            }
            if let Some(p) = s.intg_pd_ms {
                ffi::ClientReportControlBlock_setIntgPd(self.rcb, p);
                mask |= ffi::RCB_ELEMENT_INTG_PD;
            }
            if let Some(b) = s.buf_tm_ms {
                ffi::ClientReportControlBlock_setBufTm(self.rcb, b);
                mask |= ffi::RCB_ELEMENT_BUF_TM;
            }
            if !self.is_buffered() {
                ffi::ClientReportControlBlock_setResv(self.rcb, true);
                mask |= ffi::RCB_ELEMENT_RESV;
            }
            ffi::ClientReportControlBlock_setRptEna(self.rcb, true);
        }
        self.write(ied, mask)
    }

    pub fn gi(&self, ied: &Ied) -> Result<(), String> {
        // SAFETY: rcb valid
        unsafe { ffi::ClientReportControlBlock_setGI(self.rcb, true) };
        self.write(ied, ffi::RCB_ELEMENT_GI)
    }

    fn write(&self, ied: &Ied, mask: u32) -> Result<(), String> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi dan rcb valid; true = satu request
        unsafe { ffi::IedConnection_setRCBValues(ied.raw(), &mut err, self.rcb, mask, true) };
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

/// Salin LinkedList<char*> ke Vec<String>, lalu bebaskan list-nya.
fn take_strings(list: ffi::LinkedList, err: ffi::IedClientError) -> Result<Vec<String>, String> {
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
