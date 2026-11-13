//! Fase 6: muat mapping titik IEC 61850 <-> IOA virtual dari Postgres, lalu tampilkan.
//!
//! Pemakaian: mapping
//! Env (nama sama seperti FEP IEC 104). Pakai DB LOKAL/DEV, jangan DB produksi:
//!   POSTGRE_HOST (default localhost), POSTGRE_PORT (5432), DB_USERNAME (postgres),
//!   DB_PASSWORD (kosong), DB_DATABASE (ICONICS_DEV)

use iec61850_hello::mapping::{default_value_attribute, Point, PointMap};
use tokio_postgres::{NoTls, Row};

const QUERY: &str = "
SELECT md.id_rtu, md.ioa, md.tag, x.object_reference, x.functional_constraint, x.cdc,
       x.value_attribute, x.ctl_model, x.sbo_timeout_ms, x.interlock_check, x.synchro_check, x.deadband
FROM master_datapoint_iec61850s x
JOIN master_datapoints md ON md.id = x.id_master_datapoint
JOIN iec61850s r          ON r.id_rtu = md.id_rtu
WHERE md.is_deleted = FALSE AND r.is_deleted = FALSE AND md.protocol = 'IEC61850'
ORDER BY md.id_rtu, md.ioa";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn to_point(row: &Row) -> Result<Point, tokio_postgres::Error> {
    let cdc: String = row.try_get(5)?;
    let value_attribute: Option<String> = row.try_get(6)?;
    let value_attribute = value_attribute
        .or_else(|| default_value_attribute(&cdc).map(String::from))
        .unwrap_or_default();
    Ok(Point {
        id_rtu: row.try_get(0)?,
        ioa: row.try_get(1)?,
        tag: row.try_get(2)?,
        object_reference: row.try_get(3)?,
        fc: row.try_get(4)?,
        cdc,
        value_attribute,
        ctl_model: row.try_get(7)?,
        sbo_timeout_ms: row.try_get(8)?,
        interlock_check: row.try_get(9)?,
        synchro_check: row.try_get(10)?,
        deadband: row.try_get(11)?,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let port: u16 = env_or("POSTGRE_PORT", "5432")
        .parse()
        .map_err(|_| "POSTGRE_PORT tidak valid".to_string())?;
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(env_or("POSTGRE_HOST", "localhost"))
        .port(port)
        .user(env_or("DB_USERNAME", "postgres"))
        .password(env_or("DB_PASSWORD", ""))
        .dbname(env_or("DB_DATABASE", "ICONICS_DEV"));

    let (client, conn) = cfg.connect(NoTls).await.map_err(|e| format!("gagal konek DB: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("koneksi DB error: {e}");
        }
    });

    let rows = client.query(QUERY, &[]).await.map_err(|e| format!("query gagal: {e}"))?;
    let mut map = PointMap::default();
    for row in &rows {
        let p = to_point(row).map_err(|e| format!("baris tidak valid: {e}"))?;
        // Duplikat tidak menghentikan proses; cukup dilaporkan.
        if let Err(e) = map.insert(p) {
            eprintln!("WARN {e}");
        }
    }

    println!("{} titik termuat\n", map.len());
    println!("{:>6} {:>5}  {:<14} {:<2} {:<4} {:<12} REFERENCE", "RTU", "IOA", "TAG", "FC", "CDC", "ATTR");
    for p in map.iter() {
        println!(
            "{:>6} {:>5}  {:<14} {:<2} {:<4} {:<12} {}",
            p.id_rtu,
            p.ioa,
            p.tag.as_deref().unwrap_or("-"),
            p.fc,
            p.cdc,
            p.value_attribute,
            p.object_reference
        );
    }

    // Contoh terjemahan data masuk -> IOA
    let sample = "simpleIOGenericIO/GGIO1.AnIn1.mag.f";
    match map.by_reference(9001, sample, "MX") {
        Some((p, rest)) => println!("\ncontoh: RTU 9001 {sample}[MX] -> IOA {} (atribut '{rest}')", p.ioa),
        None => println!("\ncontoh: RTU 9001 {sample}[MX] tidak dimapping"),
    }
    Ok(())
}
