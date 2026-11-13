//! Fase 2: konek ke simulator IED dan baca SATU nilai.
//!
//! Pemakaian: iec61850-hello [host] [port] [object_reference]
//! Default  : ied-sim-1 102 simpleIOGenericIO/GGIO1.AnIn1.mag.f (FC = MX)

use iec61850_hello::{check, cstring, ffi, Ied};

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "ied-sim-1".into());
    let port: i32 = args
        .next()
        .map_or(Ok(102), |p| p.parse().map_err(|_| "port tidak valid".to_string()))?;
    let reference = args
        .next()
        .unwrap_or_else(|| "simpleIOGenericIO/GGIO1.AnIn1.mag.f".into());

    let ied =
        Ied::connect(&host, port).map_err(|e| format!("gagal konek ke {host}:{port}: {e}"))?;
    println!("terkoneksi ke {host}:{port}");

    let c_ref = cstring(&reference)?;
    let mut err = ffi::IED_ERROR_OK;
    // SAFETY: koneksi valid selama `ied` hidup, c_ref hidup selama pemanggilan
    let value = unsafe {
        ffi::IedConnection_readFloatValue(ied.raw(), &mut err, c_ref.as_ptr(), ffi::IEC61850_FC_MX)
    };
    check(err).map_err(|e| format!("gagal baca {reference}: {e}"))?;

    println!("{reference} [MX] = {value}");
    Ok(())
}
