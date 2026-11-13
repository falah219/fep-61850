//! Pembungkus kecil di atas FFI libIEC61850 yang dipakai bersama oleh binary di crate ini.

pub mod ffi;

use std::ffi::{CStr, CString};

/// Koneksi MMS ke satu IED. Otomatis close + destroy saat di-drop.
pub struct Ied(ffi::IedConnection);

impl Ied {
    pub fn connect(host: &str, port: i32) -> Result<Self, String> {
        let c_host = cstring(host)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: handle baru dari library; kalau konek gagal, Drop tetap memanggil destroy.
        let ied = Ied(unsafe { ffi::IedConnection_create() });
        unsafe { ffi::IedConnection_connect(ied.0, &mut err, c_host.as_ptr(), port) };
        check(err).map(|_| ied)
    }

    pub fn raw(&self) -> ffi::IedConnection {
        self.0
    }

    pub fn is_connected(&self) -> bool {
        // SAFETY: koneksi valid selama &self hidup
        unsafe { ffi::IedConnection_getState(self.0) == ffi::IED_STATE_CONNECTED }
    }

    /// Daftar nama Logical Device.
    pub fn logical_devices(&self) -> Result<Vec<String>, String> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid selama &self hidup
        let list = unsafe { ffi::IedConnection_getLogicalDeviceList(self.0, &mut err) };
        take(list, err)
    }

    /// Daftar nama Logical Node di dalam satu LD.
    pub fn logical_nodes(&self, ld: &str) -> Result<Vec<String>, String> {
        let c = cstring(ld)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan
        let list =
            unsafe { ffi::IedConnection_getLogicalDeviceDirectory(self.0, &mut err, c.as_ptr()) };
        take(list, err)
    }

    /// Isi LN untuk satu kelas ACSI (DO, dataset, BRCB, URCB, ...). `ln_ref` contoh `LD/GGIO1`.
    pub fn ln_directory(&self, ln_ref: &str, class: ffi::ACSIClass) -> Result<Vec<String>, String> {
        let c = cstring(ln_ref)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan
        let list = unsafe {
            ffi::IedConnection_getLogicalNodeDirectory(self.0, &mut err, c.as_ptr(), class)
        };
        take(list, err)
    }

    /// Anak langsung dari DO beserta FC-nya, contoh `stVal[ST]`, `ctlModel[CF]`.
    pub fn data_directory_fc(&self, do_ref: &str) -> Result<Vec<String>, String> {
        let c = cstring(do_ref)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c hidup selama pemanggilan
        let list = unsafe { ffi::IedConnection_getDataDirectoryFC(self.0, &mut err, c.as_ptr()) };
        take(list, err)
    }

    /// Anggota dataset + flag apakah dataset bisa dihapus. `ds_ref` contoh `LD/LLN0.Events`.
    pub fn dataset_directory(&self, ds_ref: &str) -> Result<(Vec<String>, bool), String> {
        let c = cstring(ds_ref)?;
        let mut err = ffi::IED_ERROR_OK;
        let mut deletable = false;
        // SAFETY: koneksi valid, c dan deletable hidup selama pemanggilan
        let list = unsafe {
            ffi::IedConnection_getDataSetDirectory(self.0, &mut err, c.as_ptr(), &mut deletable)
        };
        take(list, err).map(|m| (m, deletable))
    }
}

impl Drop for Ied {
    fn drop(&mut self) {
        // SAFETY: handle dari IedConnection_create, hanya di-drop sekali. close aman walau belum konek.
        unsafe {
            ffi::IedConnection_close(self.0);
            ffi::IedConnection_destroy(self.0);
        }
    }
}

/// Satu nilai anggota dataset. `text` untuk tampilan & perbandingan, `num` hanya untuk float (deadband).
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub text: String,
    pub num: Option<f64>,
}

/// Pembaca dataset berulang. ClientDataSet dipakai ulang tiap siklus (tidak alokasi ulang).
pub struct DataSetReader {
    reference: CString,
    ds: ffi::ClientDataSet,
}

impl DataSetReader {
    pub fn new(reference: &str) -> Result<Self, String> {
        Ok(Self { reference: cstring(reference)?, ds: std::ptr::null_mut() })
    }

    /// Baca SEMUA anggota dataset dalam satu request MMS.
    pub fn read(&mut self, ied: &Ied) -> Result<Vec<Sample>, String> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid; self.ds null (buat baru) atau milik kita dari panggilan sebelumnya.
        let ds = unsafe {
            ffi::IedConnection_readDataSetValues(ied.0, &mut err, self.reference.as_ptr(), self.ds)
        };
        if !ds.is_null() {
            self.ds = ds;
        }
        check(err)?;
        if self.ds.is_null() {
            return Err("dataset kosong".into());
        }
        // SAFETY: values dimiliki ClientDataSet, hanya dibaca di sini (tidak disimpan).
        unsafe {
            let values = ffi::ClientDataSet_getValues(self.ds);
            if values.is_null() {
                return Err("dataset tidak berisi nilai".into());
            }
            let n = ffi::MmsValue_getArraySize(values);
            Ok((0..n).map(|i| sample(ffi::MmsValue_getElement(values, i as i32))).collect())
        }
    }
}

impl Drop for DataSetReader {
    fn drop(&mut self) {
        if !self.ds.is_null() {
            // SAFETY: ds milik kita, dihancurkan sekali
            unsafe { ffi::ClientDataSet_destroy(self.ds) };
        }
    }
}

/// Ubah satu MmsValue menjadi Sample.
///
/// # Safety
/// `v` harus null atau MmsValue valid selama pemanggilan.
pub unsafe fn sample(v: *mut ffi::MmsValue) -> Sample {
    if v.is_null() {
        return Sample { text: "<null>".into(), num: None };
    }
    let mut buf = [0 as std::os::raw::c_char; 256];
    // SAFETY: buffer lokal, ukurannya dikirim ke library
    let text = unsafe {
        ffi::MmsValue_printToBuffer(v, buf.as_mut_ptr(), buf.len() as i32);
        CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned()
    };
    let num = (unsafe { ffi::MmsValue_getType(v) } == ffi::MMS_FLOAT)
        .then(|| unsafe { ffi::MmsValue_toDouble(v) });
    Sample { text, num }
}

/// Satu Report Control Block (BRCB `.BR.` atau URCB `.RP.`) yang dibaca dari IED.
pub struct Rcb {
    pub reference: String,
    c_ref: CString,
    rcb: ffi::ClientReportControlBlock,
}

impl Rcb {
    /// Baca nilai RCB saat ini dari IED (dataset, RptID, TrgOps, ...).
    pub fn read(ied: &Ied, reference: &str) -> Result<Self, String> {
        let c_ref = cstring(reference)?;
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi valid, c_ref hidup selama pemanggilan; null = buat objek RCB baru
        let rcb = unsafe {
            ffi::IedConnection_getRCBValues(ied.0, &mut err, c_ref.as_ptr(), std::ptr::null_mut())
        };
        let me = Self { reference: reference.into(), c_ref, rcb };
        check(err)?;
        if me.rcb.is_null() {
            return Err("RCB tidak ditemukan".into());
        }
        Ok(me)
    }

    pub fn is_buffered(&self) -> bool {
        // SAFETY: rcb valid (dicek non-null di read)
        unsafe { ffi::ClientReportControlBlock_isBuffered(self.rcb) }
    }

    /// Dataset yang terpasang di RCB, dalam format reference biasa (`LD/LLN0.Events`).
    pub fn dataset(&self) -> Option<String> {
        // SAFETY: string milik rcb, langsung disalin
        let p = unsafe { ffi::ClientReportControlBlock_getDataSetReference(self.rcb) };
        if p.is_null() {
            return None;
        }
        // MMS memakai `$` (LD/LLN0$Events), ACSI memakai `.`
        let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().replace('$', ".");
        (!s.is_empty()).then_some(s)
    }

    /// Pasang callback report. Harus sebelum `enable`.
    ///
    /// # Safety
    /// `param` harus tetap valid selama koneksi `ied` hidup (callback dipanggil dari thread libIEC61850).
    pub unsafe fn install_handler(
        &self,
        ied: &Ied,
        handler: ffi::ReportCallbackFunction,
        param: *mut std::os::raw::c_void,
    ) {
        // SAFETY: rptId milik rcb dan disalin oleh library; syarat param dijamin pemanggil
        unsafe {
            let rpt_id = ffi::ClientReportControlBlock_getRptId(self.rcb);
            ffi::IedConnection_installReportHandler(ied.0, self.c_ref.as_ptr(), rpt_id, handler, param);
        }
    }

    /// Aktifkan report. `None` = parameter itu TIDAK ditulis (pakai setting IED).
    pub fn enable(&self, ied: &Ied, trg_ops: Option<i32>, intg_pd_ms: Option<u32>) -> Result<(), String> {
        let mut mask = ffi::RCB_ELEMENT_RPT_ENA;
        // SAFETY: rcb valid; setter hanya mengubah objek lokal sebelum ditulis ke IED
        unsafe {
            if let Some(t) = trg_ops {
                ffi::ClientReportControlBlock_setTrgOps(self.rcb, t);
                mask |= ffi::RCB_ELEMENT_TRG_OPS;
            }
            if let Some(p) = intg_pd_ms {
                ffi::ClientReportControlBlock_setIntgPd(self.rcb, p);
                mask |= ffi::RCB_ELEMENT_INTG_PD;
            }
            if !self.is_buffered() {
                // URCB harus di-reserve dulu supaya tidak direbut client lain
                ffi::ClientReportControlBlock_setResv(self.rcb, true);
                mask |= ffi::RCB_ELEMENT_RESV;
            }
            ffi::ClientReportControlBlock_setRptEna(self.rcb, true);
        }
        self.write(ied, mask)
    }

    /// Minta General Interrogation: IED mengirim semua nilai dataset sekarang.
    pub fn gi(&self, ied: &Ied) -> Result<(), String> {
        // SAFETY: rcb valid
        unsafe { ffi::ClientReportControlBlock_setGI(self.rcb, true) };
        self.write(ied, ffi::RCB_ELEMENT_GI)
    }

    fn write(&self, ied: &Ied, mask: u32) -> Result<(), String> {
        let mut err = ffi::IED_ERROR_OK;
        // SAFETY: koneksi dan rcb valid; true = semua elemen dalam satu request
        unsafe { ffi::IedConnection_setRCBValues(ied.0, &mut err, self.rcb, mask, true) };
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

/// Teks TrgOps (`dchg,qchg,dupd,integrity,gi`) -> bitmask. String kosong = None (tidak ditulis ke IED).
pub fn parse_trg_ops(s: &str) -> Result<Option<i32>, String> {
    let mut bits = 0;
    for p in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        bits |= match p {
            "dchg" => ffi::TRG_OPT_DATA_CHANGED,
            "qchg" => ffi::TRG_OPT_QUALITY_CHANGED,
            "dupd" => ffi::TRG_OPT_DATA_UPDATE,
            "integrity" | "period" => ffi::TRG_OPT_INTEGRITY,
            "gi" => ffi::TRG_OPT_GI,
            _ => return Err(format!("trg_ops tidak dikenal: {p}")),
        };
    }
    Ok((bits != 0).then_some(bits as i32))
}

/// Reason-for-inclusion (bitmask) -> `action` Kafka. Satu anggota bisa punya beberapa bit;
/// urutan prioritas: GI > INTEGRITY > DATA_CHANGE > QUALITY_CHANGE > DATA_UPDATE.
pub fn reason_action(reason: u32) -> Option<&'static str> {
    [
        (ffi::IEC61850_REASON_GI, "GENERAL_INTERROGATION"),
        (ffi::IEC61850_REASON_INTEGRITY, "INTEGRITY"),
        (ffi::IEC61850_REASON_DATA_CHANGE, "DATA_CHANGE"),
        (ffi::IEC61850_REASON_QUALITY_CHANGE, "QUALITY_CHANGE"),
        (ffi::IEC61850_REASON_DATA_UPDATE, "DATA_UPDATE"),
    ]
    .into_iter()
    .find(|(bit, _)| reason & bit != 0)
    .map(|(_, a)| a)
}

/// Apakah `cur` perlu dikirim dibanding nilai terakhir yang DIKIRIM (`prev`)?
/// Float: hanya jika selisih > deadband. Lainnya (status, quality, struktur): jika teksnya berubah.
pub fn changed(prev: &Sample, cur: &Sample, deadband: f64) -> bool {
    match (prev.num, cur.num) {
        (Some(a), Some(b)) if !a.is_nan() && !b.is_nan() => (b - a).abs() > deadband,
        _ => prev.text != cur.text,
    }
}

/// Jadwal siklus tetap. Kembalikan deadline berikutnya + jumlah siklus yang dilewati
/// karena siklus sebelumnya terlambat (siklus tidak ditumpuk).
pub fn next_deadline(
    deadline: std::time::Instant,
    now: std::time::Instant,
    interval: std::time::Duration,
) -> (std::time::Instant, u32) {
    let mut next = deadline + interval;
    let mut skipped = 0;
    while next <= now {
        next += interval;
        skipped += 1;
    }
    (next, skipped)
}

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

/// Salin LinkedList<char*> dari library ke Vec<String>, lalu bebaskan list-nya.
fn take(list: ffi::LinkedList, err: ffi::IedClientError) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    if !list.is_null() {
        // SAFETY: list milik kita (dikembalikan library). Elemen pertama adalah kepala kosong;
        // data elemen berikutnya adalah char* hasil malloc. LinkedList_destroy membebaskan list + data.
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
    use std::time::{Duration, Instant};

    fn f(v: f64) -> Sample {
        Sample { text: v.to_string(), num: Some(v) }
    }
    fn t(s: &str) -> Sample {
        Sample { text: s.into(), num: None }
    }

    #[test]
    fn deadband_float() {
        assert!(!changed(&f(1.0), &f(1.05), 0.1));
        assert!(changed(&f(1.0), &f(1.2), 0.1));
        assert!(changed(&f(1.0), &f(0.8), 0.1));
        assert!(!changed(&f(1.0), &f(1.0), 0.0));
        assert!(changed(&f(1.0), &f(1.0001), 0.0));
    }

    #[test]
    fn nan_tetap_terdeteksi() {
        assert!(changed(&f(1.0), &f(f64::NAN), 10.0));
    }

    #[test]
    fn status_dan_quality_pakai_teks() {
        assert!(!changed(&t("true"), &t("true"), 100.0));
        assert!(changed(&t("true"), &t("false"), 100.0));
        assert!(changed(&t("0000000000000"), &t("0100000000000"), 100.0));
    }

    #[test]
    fn trg_ops_teks_ke_bit() {
        assert_eq!(parse_trg_ops("dchg,qchg,integrity,gi"), Ok(Some(1 | 2 | 8 | 16)));
        assert_eq!(parse_trg_ops(" dupd , period "), Ok(Some(4 | 8)));
        assert_eq!(parse_trg_ops(""), Ok(None));
        assert!(parse_trg_ops("dchg,xyz").is_err());
    }

    #[test]
    fn reason_ke_action() {
        assert_eq!(reason_action(0), None);
        assert_eq!(reason_action(1), Some("DATA_CHANGE"));
        assert_eq!(reason_action(2), Some("QUALITY_CHANGE"));
        assert_eq!(reason_action(4), Some("DATA_UPDATE"));
        assert_eq!(reason_action(8), Some("INTEGRITY"));
        assert_eq!(reason_action(16), Some("GENERAL_INTERROGATION"));
        assert_eq!(reason_action(1 | 2), Some("DATA_CHANGE"));
        assert_eq!(reason_action(16 | 1), Some("GENERAL_INTERROGATION"));
        assert_eq!(reason_action(32), None); // UNKNOWN: IED tidak mengirim reason code
    }

    #[test]
    fn jadwal_lewati_siklus_terlambat() {
        let i = Duration::from_millis(100);
        let d = Instant::now();
        assert_eq!(next_deadline(d, d + Duration::from_millis(50), i), (d + i, 0));
        assert_eq!(next_deadline(d, d + Duration::from_millis(250), i), (d + 3 * i, 2));
    }
}
