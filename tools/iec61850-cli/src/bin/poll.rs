//! Baca dataset berkala + deteksi perubahan (deadband) + integrity. Hanya membaca.
//! Dipakai saat commissioning untuk memastikan dataset terbaca dan nilainya masuk akal.
//!
//! Pemakaian: poll [host] [port] [dataset_ref ...]
//! Default  : ied-sim-1 102 simpleIOGenericIO/LLN0.Measurements simpleIOGenericIO/LLN0.Events2
//!
//! Env (opsional):
//!   POLL_INTERVAL_MS     default 2000   jeda antar siklus
//!   INTEGRITY_PERIOD_MS  default 30000  kirim ulang semua nilai
//!   DEADBAND             default 0.05   ambang perubahan measurement (absolut)

use std::time::{Duration, Instant};

use iec61850_cli::{changed, next_deadline, DataSetReader, Ied, Sample};

struct Polled {
    name: String,
    members: Vec<String>,
    reader: DataSetReader,
    last_sent: Vec<Option<Sample>>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> Result<T, String> {
    match std::env::var(key) {
        Ok(v) => v.parse().map_err(|_| format!("{key} tidak valid: {v}")),
        Err(_) => Ok(default),
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "ied-sim-1".into());
    let port: i32 = args
        .next()
        .map_or(Ok(102), |p| p.parse().map_err(|_| "port tidak valid".to_string()))?;
    let mut refs: Vec<String> = args.collect();
    if refs.is_empty() {
        refs = vec![
            "simpleIOGenericIO/LLN0.Measurements".into(),
            "simpleIOGenericIO/LLN0.Events2".into(),
        ];
    }

    let interval = Duration::from_millis(env_or("POLL_INTERVAL_MS", 2000)?);
    let integrity = Duration::from_millis(env_or("INTEGRITY_PERIOD_MS", 30000)?);
    let deadband: f64 = env_or("DEADBAND", 0.05)?;

    let ied =
        Ied::connect(&host, port).map_err(|e| format!("gagal konek ke {host}:{port}: {e}"))?;
    println!(
        "polling {host}:{port} interval={}ms integrity={}ms deadband={deadband}",
        interval.as_millis(),
        integrity.as_millis()
    );

    let mut sets = Vec::new();
    for r in refs {
        // Nama anggota dibaca sekali di awal; urutannya sama dengan urutan nilai saat dibaca.
        let (members, _) = ied
            .dataset_directory(&r)
            .map_err(|e| format!("gagal baca directory {r}: {e}"))?;
        println!("  dataset {r}: {} anggota", members.len());
        sets.push(Polled {
            last_sent: vec![None; members.len()],
            reader: DataSetReader::new(&r)?,
            name: r,
            members,
        });
    }

    let start = Instant::now();
    let mut deadline = start;
    let mut last_integrity = start;

    loop {
        let is_integrity = last_integrity.elapsed() >= integrity;
        if is_integrity {
            last_integrity = Instant::now();
        }

        // Satu siklus: dataset dibaca berurutan (request MMS satu per satu).
        for set in &mut sets {
            let values = match set.reader.read(&ied) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("[{:>9.3}s] WARN gagal baca {}: {e}", start.elapsed().as_secs_f64(), set.name);
                    continue;
                }
            };
            for (i, cur) in values.into_iter().enumerate() {
                let Some(slot) = set.last_sent.get_mut(i) else { break };
                let action = match slot {
                    _ if is_integrity => "INTEGRITY",
                    None => "POLLING",
                    Some(prev) if changed(prev, &cur, deadband) => "POLLING",
                    Some(_) => continue,
                };
                let name = set.members.get(i).map_or("?", String::as_str);
                println!("[{:>9.3}s] {action:<9} {name} = {}", start.elapsed().as_secs_f64(), cur.text);
                *slot = Some(cur);
            }
        }

        let now = Instant::now();
        let (next, skipped) = next_deadline(deadline, now, interval);
        if skipped > 0 {
            eprintln!(
                "[{:>9.3}s] WARN siklus terlambat, {skipped} siklus dilewati",
                start.elapsed().as_secs_f64()
            );
        }
        deadline = next;
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
    }
}
