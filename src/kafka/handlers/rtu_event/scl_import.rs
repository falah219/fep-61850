//! Event SCL_IMPORT (RTU_EVENTS): file SCL yang disimpan backend (`iec61850_scl_files`) -> katalog model IED
//! (format sama dengan BROWSE, `scl::catalog`) -> IEC61850_BROWSE_RESPONSE `source: "SCL"`.
//!
//! TIDAK konek / menulis ke IED, tidak menyentuh connection manager, polling, RCB, atau RTU_STATUS, jadi jalan
//! walau RTU OFF / tidak berjalan. Tidak menulis tabel konfigurasi: mapping dihitung backend.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;
use tracing::{info, warn};

use super::browse::{responses, SclMeta, MAX_PART_BYTES};
use crate::connection::Manager;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::RtuEventMessage;
use crate::kafka::TOPIC_BROWSE_RESPONSE;
use crate::repository::{load_scl_file, SclFileRow};
use crate::scl::{self, IedRef, SclCatalog};

/// key yang sama dalam jendela ini diabaikan.
const DEDUP_WINDOW: Duration = Duration::from_secs(600);
/// Default SCL_IMPORT_TIMEOUT_MS.
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
/// Default SCL_MAX_BYTES (20 MB).
const DEFAULT_MAX_BYTES: u64 = 20 * 1024 * 1024;

fn env_u64(k: &str, default: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.trim().parse().ok()).filter(|v| *v > 0).unwrap_or(default)
}

/// Import gagal. `file_name` diisi kalau file sudah terbaca dari DB.
#[derive(Debug, Default, PartialEq)]
struct Failed {
    message: String,
    ied_names: Option<Vec<IedRef>>,
    file_name: Option<String>,
}

impl Failed {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), ..Default::default() }
    }
}

type Imported = Result<(Option<String>, SclCatalog), Failed>;

/// false = key sudah dipakai dalam DEDUP_WINDOW terakhir.
fn first_time(key: &str, now: Instant) -> bool {
    static SEEN: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    let mut seen = SEEN.get_or_init(Default::default).lock().unwrap_or_else(PoisonError::into_inner);
    seen.retain(|_, t| now.duration_since(*t) < DEDUP_WINDOW);
    if seen.contains_key(key) {
        return false;
    }
    seen.insert(key.to_string(), now);
    true
}

/// Bagian SINKRON (log + dedupe). None = key dobel, diabaikan. Future-nya di-spawn pemanggil.
pub fn submit(mgr: &Arc<Manager>, m: RtuEventMessage) -> Option<impl Future<Output = ()> + Send + 'static> {
    let data = m.data.as_ref();
    let id = m.id_rtu.or_else(|| data_id_rtu(data));
    let key = m.key.clone().or_else(|| data.and_then(|d| d.get("key")).and_then(Value::as_str).map(String::from)).unwrap_or_default();
    let id_scl_file = m.id_scl_file.filter(|s| !s.trim().is_empty());
    let ied_name = m.ied_name.filter(|s| !s.trim().is_empty());
    info!("[RTU_EVENTS] event=SCL_IMPORT id_rtu={id:?} key={key:?} id_scl_file={id_scl_file:?}");
    if !key.is_empty() && !first_time(&key, Instant::now()) {
        info!("[SCL_IMPORT] key {key:?} sudah diproses dalam 10 menit terakhir, diabaikan");
        return None;
    }
    let mgr = Arc::clone(mgr);
    Some(async move {
        let started = Instant::now();
        // IP hanya untuk RTU_LOGGING (RTU boleh tidak berjalan)
        let ip = id.and_then(|i| mgr.ip_of(i).or_else(|| mgr.db_row(i).map(|r| r.ip_address))).unwrap_or_default();
        let log = |level: &str, msg: &str| {
            if let Some(i) = id {
                mgr.publisher.producer.send_log(&ip, i, level, msg);
            }
        };
        log("INFO", &format!("Import SCL mulai: file {}", id_scl_file.as_deref().unwrap_or("-")));
        let timeout = Duration::from_millis(env_u64("SCL_IMPORT_TIMEOUT_MS", DEFAULT_TIMEOUT_MS));
        let out = tokio::time::timeout(timeout, import(&mgr, id, id_scl_file.as_deref(), ied_name))
            .await
            .unwrap_or_else(|_| Err(Failed::new("Import SCL timeout")));
        let ms = started.elapsed().as_millis();
        match &out {
            Ok((_, c)) => {
                let r = &c.result;
                let msg = format!("Import SCL selesai: {} objek, {} RCB, {} error ({ms} ms)", r.objects.len(), r.rcbs.len(), r.errors.len());
                info!("[SCL_IMPORT] id_rtu={id:?} {msg}");
                log("INFO", &msg);
            }
            Err(f) => log("WARN", &format!("Import SCL gagal: {} ({ms} ms)", f.message)),
        }
        let topic_key = if key.is_empty() { "unknown" } else { key.as_str() };
        for msg in messages(&key, id, id_scl_file.as_deref(), out) {
            mgr.publisher.producer.send_raw(TOPIC_BROWSE_RESPONSE, topic_key, &msg);
        }
    })
}

async fn import(mgr: &Manager, id: Option<i32>, id_scl_file: Option<&str>, ied_name: Option<String>) -> Imported {
    let id = id.ok_or_else(|| Failed::new("id_rtu wajib diisi"))?;
    let file_id = id_scl_file.ok_or_else(|| Failed::new("id_scl_file wajib diisi"))?;
    let max = env_u64("SCL_MAX_BYTES", DEFAULT_MAX_BYTES);
    // id bukan UUID = pasti tidak ada (tanpa error cast dari DB)
    let row = if is_uuid(file_id) {
        let c = mgr.pool.get().await.map_err(|e| Failed::new(format!("DB tidak tersedia: {e}")))?;
        load_scl_file(&c, file_id, id, i64::try_from(max).unwrap_or(i64::MAX)).await.map_err(Failed::new)?
    } else {
        None
    };
    let (file_name, db_ied_name, content) = check_file(row, max)?;
    // ied_name event diutamakan
    let ied_name = ied_name.or(db_ied_name.filter(|s| !s.trim().is_empty()));
    let parsed = tokio::task::spawn_blocking(move || scl::catalog(&content, ied_name.as_deref()))
        .await
        .map_err(|e| Failed::new(format!("parsing SCL berhenti: {e}")))?;
    match parsed {
        Ok(c) => Ok((file_name, c)),
        Err(e) => Err(Failed { message: e.message, ied_names: e.ied_names, file_name }),
    }
}

fn is_uuid(s: &str) -> bool {
    s.len() == 36 && s.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

/// (file_name, ied_name kolom, content).
fn check_file(row: Option<SclFileRow>, max: u64) -> Result<(Option<String>, Option<String>, String), Failed> {
    let row = row.filter(|r| !r.is_deleted).ok_or_else(|| Failed::new("File SCL tidak ditemukan"))?;
    let too_big = row.size.is_some_and(|s| s > i64::try_from(max).unwrap_or(i64::MAX))
        || row.content.as_ref().is_some_and(|c| c.len() as u64 > max);
    if too_big {
        return Err(Failed { message: "File SCL terlalu besar".into(), file_name: row.file_name, ..Default::default() });
    }
    // content NULL / kosong -> "File bukan SCL yang valid" dari parser
    Ok((row.file_name, row.ied_name, row.content.unwrap_or_default()))
}

fn to_values<T: Serialize>(msgs: &[T]) -> Vec<Value> {
    msgs.iter().filter_map(|m| serde_json::to_value(m).ok()).collect()
}

/// Pesan IEC61850_BROWSE_RESPONSE (format & pemecahan sama dengan BROWSE).
fn messages(key: &str, id: Option<i32>, id_scl_file: Option<&str>, out: Imported) -> Vec<Value> {
    match out {
        Ok((file_name, SclCatalog { ied, config_revs, result })) => {
            let meta = SclMeta { file_name: file_name.as_deref(), id_scl_file, ied: Some(&ied), config_revs: Some(config_revs.as_slice()), ied_names: None };
            let result = Ok(result);
            let parts = responses(key, id, &result, Some(meta), MAX_PART_BYTES);
            to_values(&parts)
        }
        Err(f) => {
            let meta = SclMeta { file_name: f.file_name.as_deref(), id_scl_file, ied_names: f.ied_names.as_deref(), ..Default::default() };
            let result = Err(f.message.clone());
            let parts = responses(key, id, &result, Some(meta), MAX_PART_BYTES);
            to_values(&parts)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iec61850::acquisition::Ctl;
    use serde_json::json;

    const CID: &str = include_str!("../../../../tests/fixtures/scl/baseline_1ied.cid");
    const SCD: &str = include_str!("../../../../tests/fixtures/scl/station_2ied.scd");
    const UUID: &str = "0f8e2c1a-5b6d-4e7f-9a0b-1c2d3e4f5a6b";

    fn row(content: &str) -> Option<SclFileRow> {
        Some(SclFileRow { file_name: Some("x.cid".into()), content: Some(content.into()), size: Some(content.len() as i64), ..Default::default() })
    }

    #[test]
    fn key_dobel_dalam_10_menit_diabaikan() {
        let t = Instant::now();
        assert!(first_time("scl-uji-dedupe", t));
        assert!(!first_time("scl-uji-dedupe", t + Duration::from_secs(599)));
        assert!(first_time("scl-uji-dedupe-lain", t));
        assert!(first_time("scl-uji-dedupe", t + Duration::from_secs(601)));
    }

    #[test]
    fn file_tidak_ada_dihapus_atau_terlalu_besar() {
        assert_eq!(check_file(None, 100).unwrap_err().message, "File SCL tidak ditemukan");
        let deleted = row("<SCL/>").map(|r| SclFileRow { is_deleted: true, ..r });
        assert_eq!(check_file(deleted, 100).unwrap_err().message, "File SCL tidak ditemukan");
        // > max: content tidak ditarik dari DB (NULL), size dari octet_length
        let big = Some(SclFileRow { file_name: Some("besar.scd".into()), size: Some(101), ..Default::default() });
        let e = check_file(big, 100).unwrap_err();
        assert_eq!((e.message.as_str(), e.file_name.as_deref()), ("File SCL terlalu besar", Some("besar.scd")));
        let (name, _, content) = check_file(row("<SCL/>"), 100).unwrap();
        assert_eq!((name.as_deref(), content.as_str()), (Some("x.cid"), "<SCL/>"));
        assert!(!is_uuid("bukan-uuid") && is_uuid(UUID));
    }

    #[test]
    fn respons_sukses_sama_dengan_browse_plus_field_scl() {
        let cat = scl::catalog(CID, None).unwrap();
        let out = messages("scl-9399-a", Some(9399), Some(UUID), Ok((Some("baseline.cid".into()), cat)));
        assert_eq!(out.len(), 1);
        let m = &out[0];
        assert_eq!((m["status"].as_str(), m["source"].as_str()), (Some("SUCCESS"), Some("SCL")));
        assert_eq!(m["message"], "Import SCL selesai: 9 objek, 3 RCB");
        assert_eq!((m["file_name"].as_str(), m["id_scl_file"].as_str()), (Some("baseline.cid"), Some(UUID)));
        assert_eq!(m["ied"]["ip"], "10.10.1.21");
        assert_eq!(m["config_revs"][0]["value"], "Rev 2026-11");
        assert_eq!((&m["errors"], m["part"].as_u64()), (&json!([]), Some(1)));
        assert!(m.get("ied_names").is_none());
        let s = serde_json::to_string(m).unwrap();
        let keys = ["key", "id_rtu", "status", "source", "file_name", "id_scl_file", "ied", "config_revs", "message", "objects", "rcbs", "errors", "part"];
        let pos: Vec<usize> = keys.iter().map(|k| s.find(&format!("\"{k}\":")).unwrap()).collect();
        assert!(pos.windows(2).all(|w| w[0] < w[1]), "{s}");
    }

    #[test]
    fn respons_gagal_pilih_ied() {
        let e = scl::catalog(SCD, None).unwrap_err();
        let f = Failed { message: e.message, ied_names: e.ied_names, file_name: Some("gi.scd".into()) };
        let m = &messages("scl-9399-b", Some(9399), Some(UUID), Err(f))[0];
        assert_eq!((m["status"].as_str(), m["source"].as_str(), m["message"].as_str()), (Some("FAILED"), Some("SCL"), Some("Pilih IED")));
        assert_eq!(m["ied_names"], json!([{"name": "IED1", "ip": "10.0.0.1"}, {"name": "IED2", "ip": "10.0.0.2"}]));
        assert_eq!(m["errors"], json!([{"ref": "scl", "error": "Pilih IED"}]));
        assert!(m.get("ied").is_none());
        // tanpa id_rtu: tetap dijawab
        let m = &messages("k", None, None, Err(Failed::new("id_rtu wajib diisi")))[0];
        assert_eq!((m["id_rtu"].clone(), m["message"].as_str()), (Value::Null, Some("id_rtu wajib diisi")));
    }

    /// RTU tidak berjalan (state OFF) tetap diproses, key dobel diabaikan, thread IED lain tidak menerima apa pun.
    #[tokio::test]
    async fn rtu_off_diproses_dan_ied_lain_tidak_terganggu() {
        let mgr = crate::connection::testing::manager();
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        crate::connection::testing::fake_ied(&mgr, 9001, move |rx| {
            while let Ok(ctl) = rx.recv() {
                if matches!(ctl, Ctl::Stop) {
                    break;
                }
                let _ = seen_tx.send(());
            }
        });
        let ev = |key: &str| -> RtuEventMessage {
            serde_json::from_value(json!({"event": "SCL_IMPORT", "protocol": "IEC61850", "id_rtu": 9399, "key": key,
                                          "id_scl_file": UUID, "ied_name": null}))
            .unwrap()
        };
        // 9399 tidak berjalan; DB test tidak ada -> FAILED cepat, tanpa menunggu timeout
        let job = submit(&mgr, ev("scl-uji-off")).expect("diproses walau RTU tidak berjalan");
        tokio::time::timeout(Duration::from_secs(10), job).await.unwrap();
        assert!(submit(&mgr, ev("scl-uji-off")).is_none(), "key dobel");
        assert!(mgr.config_of(9399).is_none(), "SCL_IMPORT tidak menjalankan RTU");
        assert!(seen_rx.try_recv().is_err(), "thread IED lain tidak menerima perintah");
        mgr.stop_all(Duration::from_secs(5)).await;
    }
}
