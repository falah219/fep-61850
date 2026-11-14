//! Error FEP IEC 61850 (padanan `error.rs` FEP IEC 104).
//!
//! Hanya varian yang benar-benar dipakai. `Mms` membawa kode `IedClientError` libIEC61850, jadi
//! pemanggil bisa membedakan timeout, koneksi putus, dan IED yang menolak request.

use std::ffi::CStr;
use std::fmt;

use crate::ffi;
use crate::iec61850::client::{is_connection_error, is_server_response};

pub type Result<T> = std::result::Result<T, Iec61850Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Iec61850Error {
    /// Request MMS gagal. `code` = IedClientError libIEC61850.
    Mms { code: ffi::IedClientError, msg: String },
    /// Nilai konfigurasi tidak valid (reference, FC, dataset, ...).
    InvalidConfiguration(String),
    /// Postgres
    Database(String),
    Other(String),
}

impl Iec61850Error {
    /// Error dari kode IedClientError, pesannya dari `IedClientError_toString`.
    pub fn mms(code: ffi::IedClientError) -> Self {
        // SAFETY: IedClientError_toString mengembalikan string statis milik library
        let msg = unsafe { CStr::from_ptr(ffi::IedClientError_toString(code)) }.to_string_lossy().into_owned();
        Self::Mms { code, msg }
    }

    /// Kode IedClientError kalau error ini dari request MMS.
    pub fn code(&self) -> Option<ffi::IedClientError> {
        match self {
            Self::Mms { code, .. } => Some(*code),
            _ => None,
        }
    }

    pub fn is_timeout(&self) -> bool {
        self.code() == Some(ffi::IED_ERROR_TIMEOUT)
    }

    /// Koneksi MMS mati: sesi harus diakhiri lalu reconnect.
    pub fn is_connection_lost(&self) -> bool {
        self.code().is_some_and(is_connection_error)
    }

    /// IED menjawab dengan error (objek tidak ada, akses ditolak, ...): koneksinya hidup.
    pub fn is_server_response(&self) -> bool {
        self.code().is_some_and(is_server_response)
    }
}

impl fmt::Display for Iec61850Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mms { msg, .. } => f.write_str(msg),
            Self::InvalidConfiguration(m) => write!(f, "konfigurasi tidak valid: {m}"),
            Self::Database(m) | Self::Other(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Iec61850Error {}

impl From<String> for Iec61850Error {
    fn from(m: String) -> Self {
        Self::Other(m)
    }
}

impl From<&str> for Iec61850Error {
    fn from(m: &str) -> Self {
        Self::Other(m.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn klasifikasi_dari_kode() {
        let t = Iec61850Error::mms(ffi::IED_ERROR_TIMEOUT);
        assert!(t.is_timeout() && !t.is_connection_lost() && !t.is_server_response());
        assert!(!t.to_string().is_empty()); // pesan dari libIEC61850

        assert!(Iec61850Error::mms(ffi::IED_ERROR_CONNECTION_LOST).is_connection_lost());
        assert!(Iec61850Error::mms(ffi::IED_ERROR_NOT_CONNECTED).is_connection_lost());
        assert!(Iec61850Error::mms(ffi::IED_ERROR_OBJECT_DOES_NOT_EXIST).is_server_response());

        let other: Iec61850Error = "x".into();
        assert_eq!(other.code(), None);
        assert!(!other.is_timeout());
        assert_eq!(Iec61850Error::InvalidConfiguration("FC XX".into()).to_string(), "konfigurasi tidak valid: FC XX");
    }
}
