//! Fase 10: SCL (ICD/CID/SCD) -> usulan mapping + IOA virtual, ditulis ke file SQL & CSV untuk direview.
//!
//! Pemakaian: scl-import <file.scd> <id_rtu> [nama_IED]
//! Hasil   : <nama_file>.<id_rtu>.sql dan <nama_file>.<id_rtu>.csv di folder kerja.

use fep_iec61850::scl;

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [file, id_rtu, rest @ ..] = args.as_slice() else {
        return Err("pemakaian: scl-import <file.scd> <id_rtu> [nama_IED]".into());
    };
    let id_rtu: i32 = id_rtu.parse().map_err(|_| format!("id_rtu tidak valid: {id_rtu}"))?;
    let xml = std::fs::read_to_string(file).map_err(|e| format!("gagal baca {file}: {e}"))?;

    let ied = scl::parse(&xml, rest.first().map(String::as_str))?;
    // Ditulis di folder kerja (di container dev = /work, terlihat dari Windows)
    let stem = std::path::Path::new(file).file_stem().and_then(|s| s.to_str()).unwrap_or("scl");
    let sql_path = format!("{stem}.{id_rtu}.sql");
    let csv_path = format!("{stem}.{id_rtu}.csv");
    std::fs::write(&sql_path, scl::to_sql(&ied, id_rtu, file)).map_err(|e| format!("gagal tulis {sql_path}: {e}"))?;
    std::fs::write(&csv_path, scl::to_csv(&ied, id_rtu)).map_err(|e| format!("gagal tulis {csv_path}: {e}"))?;

    let count = |k| ied.points.iter().filter(|p| p.kind == k).count();
    println!("IED {}: {} titik", ied.name, ied.points.len());
    println!("  status      {:>4}  (IOA {}..)", count(scl::Kind::Status), scl::IOA_STATUS);
    println!("  measurement {:>4}  (IOA {}..)", count(scl::Kind::Measurement), scl::IOA_MEASUREMENT);
    println!("  kontrol     {:>4}  (IOA {}..)", count(scl::Kind::Control), scl::IOA_CONTROL);
    println!("  RCB         {:>4}  (dikomentari di SQL, aktifkan yang dipakai)", ied.rcbs.len());
    println!("ditulis: {sql_path}\n         {csv_path}");
    Ok(())
}
