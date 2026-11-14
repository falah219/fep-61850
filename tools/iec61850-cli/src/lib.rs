//! Pembungkus kecil di atas FFI libIEC61850 untuk alat bantu `browse` dan `poll`.

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
    fn jadwal_lewati_siklus_terlambat() {
        let i = Duration::from_millis(100);
        let d = Instant::now();
        assert_eq!(next_deadline(d, d + Duration::from_millis(50), i), (d + i, 0));
        assert_eq!(next_deadline(d, d + Duration::from_millis(250), i), (d + 3 * i, 2));
    }
}
