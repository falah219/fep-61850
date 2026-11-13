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
