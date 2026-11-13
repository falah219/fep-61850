//! Fase 2: konek ke simulator IED dan baca SATU nilai.
//!
//! Pemakaian: iec61850-hello [host] [port] [object_reference]
//! Default  : ied-sim-1 102 simpleIOGenericIO/GGIO1.AnIn1.mag.f (FC = MX)

mod ffi;

use std::ffi::{CStr, CString};

fn err_str(err: ffi::IedClientError) -> String {
    // SAFETY: IedClientError_toString mengembalikan string statis milik library
    unsafe { CStr::from_ptr(ffi::IedClientError_toString(err)) }
        .to_string_lossy()
        .into_owned()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "ied-sim-1".into());
    let port: i32 = match args.next().map(|p| p.parse()) {
        None => 102,
        Some(Ok(p)) => p,
        Some(Err(_)) => return eprintln!("port tidak valid"),
    };
    let reference = args
        .next()
        .unwrap_or_else(|| "simpleIOGenericIO/GGIO1.AnIn1.mag.f".into());

    let (Ok(c_host), Ok(c_ref)) = (CString::new(host.clone()), CString::new(reference.clone()))
    else {
        return eprintln!("argumen mengandung karakter NUL");
    };

    let mut err: ffi::IedClientError = ffi::IED_ERROR_OK;

    // SAFETY: con dibuat, dipakai, lalu dihancurkan di fungsi ini saja;
    // semua pointer string hidup selama pemanggilan.
    unsafe {
        let con = ffi::IedConnection_create();
        ffi::IedConnection_connect(con, &mut err, c_host.as_ptr(), port);

        if err != ffi::IED_ERROR_OK {
            eprintln!("gagal konek ke {host}:{port}: {}", err_str(err));
        } else {
            println!("terkoneksi ke {host}:{port}");
            let value =
                ffi::IedConnection_readFloatValue(con, &mut err, c_ref.as_ptr(), ffi::IEC61850_FC_MX);
            if err == ffi::IED_ERROR_OK {
                println!("{reference} [MX] = {value}");
            } else {
                eprintln!("gagal baca {reference}: {}", err_str(err));
            }
            ffi::IedConnection_close(con);
        }

        ffi::IedConnection_destroy(con);
    }
}
