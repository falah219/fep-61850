//! Tampilkan model IED: LD -> LN -> DO (+atribut & FC), dataset, dan RCB. Hanya membaca.
//! Dipakai saat commissioning untuk mengisi object_reference, dataset, dan rcb_reference.
//!
//! Pemakaian: browse [host] [port]   (default: ied-sim-1 102)

use iec61850_cli::{ffi, Ied};

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

    let ied =
        Ied::connect(&host, port).map_err(|e| format!("gagal konek ke {host}:{port}: {e}"))?;
    println!("IED {host}:{port}");

    for ld in ied.logical_devices()? {
        println!("\nLD  {ld}");

        for ln in ied.logical_nodes(&ld)? {
            let ln_ref = format!("{ld}/{ln}");
            println!("  LN  {ln}");

            for name in ied.ln_directory(&ln_ref, ffi::ACSI_CLASS_DATA_OBJECT)? {
                let attrs = ied.data_directory_fc(&format!("{ln_ref}.{name}"))?;
                println!("      DO  {name:<12} {}", attrs.join(" "));
            }

            for ds in ied.ln_directory(&ln_ref, ffi::ACSI_CLASS_DATA_SET)? {
                let (members, deletable) = ied.dataset_directory(&format!("{ln_ref}.{ds}"))?;
                let kind = if deletable { "dinamis" } else { "tetap" };
                println!("      DS  {ds} ({kind}, {} anggota)", members.len());
                for m in members {
                    println!("            {m}");
                }
            }

            for (class, fc) in [(ffi::ACSI_CLASS_BRCB, "BR"), (ffi::ACSI_CLASS_URCB, "RP")] {
                for rcb in ied.ln_directory(&ln_ref, class)? {
                    println!("      RCB {ln_ref}.{fc}.{rcb}");
                }
            }
        }
    }
    Ok(())
}
