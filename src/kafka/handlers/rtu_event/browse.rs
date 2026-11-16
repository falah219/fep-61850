//! Event BROWSE (RTU_EVENTS): browse model IED di thread IED -> IEC61850_BROWSE_RESPONSE.
//! IED besar bisa ribuan objek -> dikirim per bagian (`part` 1..=`total_parts`) maks ~900 KB.
//! `errors` / `rcbs` (opsional) hanya di bagian 1.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::connection::Manager;
use crate::iec61850::acquisition::Ctl;
use crate::iec61850::browse::{browse_timeout, BrowseError, BrowseObject, BrowseResult, RcbInfo};
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::RtuEventMessage;
use crate::kafka::TOPIC_BROWSE_RESPONSE;

/// Di atas BROWSE_TIMEOUT_MS: thread IED membalas hasil sebagian sendiri saat batas waktunya tercapai.
const REPLY_MARGIN: Duration = Duration::from_secs(30);
/// Di bawah batas default Kafka 1 MB (message.max.bytes), sisa untuk field lain.
const MAX_PART_BYTES: usize = 900 * 1024;

fn is_empty<T>(s: &&[T]) -> bool {
    s.is_empty()
}

#[derive(Debug, Serialize)]
struct BrowseResponse<'a> {
    key: &'a str,
    id_rtu: Option<i32>,
    status: &'static str,
    message: &'a str,
    objects: &'a [BrowseObject],
    part: usize,
    total_parts: usize,
    /// Node yang gagal dibrowse (bagian 1 saja)
    #[serde(skip_serializing_if = "is_empty")]
    errors: &'a [BrowseError],
    /// RCB + anggota dataset (bagian 1 saja)
    #[serde(skip_serializing_if = "is_empty")]
    rcbs: &'a [RcbInfo],
    /// Hanya ada (false) kalau BROWSE_TIMEOUT_MS tercapai sebelum semua LN dibrowse
    #[serde(skip_serializing_if = "Option::is_none")]
    complete: Option<bool>,
}

/// Bagian SINKRON: kirim ke antrean thread IED. Future (tunggu + kirim respons) boleh di-spawn.
pub fn submit(mgr: &Arc<Manager>, m: RtuEventMessage) -> impl Future<Output = ()> + Send + 'static {
    let data = m.data.as_ref();
    // sama dengan COMMAND: id_rtu atas -> data.id_rtu -> dicari dari ip_address (data -> atas)
    let ip = data.and_then(|d| d.get("ip_address")).and_then(Value::as_str).unwrap_or(&m.ip_address).to_string();
    let id = m.id_rtu.or_else(|| data_id_rtu(data)).or_else(|| (!ip.is_empty()).then(|| mgr.id_by_ip(&ip)).flatten());
    let key = m.key.clone().or_else(|| data.and_then(|d| d.get("key")).and_then(Value::as_str).map(String::from)).unwrap_or_default();
    info!("[BROWSE] id_rtu={id:?} ip={ip} key={key:?}");
    let queued = match id {
        None if ip.is_empty() => Err("id_rtu atau ip_address wajib diisi".to_string()),
        None => Err(format!("RTU tidak ditemukan untuk IP {ip}")),
        Some(id) => {
            let (tx, rx) = oneshot::channel();
            if mgr.send(id, Ctl::Browse(tx)) { Ok(rx) } else { Err("IED tidak terkoneksi".into()) }
        }
    };
    let mgr = Arc::clone(mgr);
    async move {
        let result = match queued {
            Ok(rx) => match tokio::time::timeout(browse_timeout() + REPLY_MARGIN, rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => Err("thread IED berhenti sebelum browse selesai".into()),
                Err(_) => Err("timeout menunggu hasil browse".into()),
            },
            Err(e) => Err(e),
        };
        let topic_key = if key.is_empty() { "unknown" } else { key.as_str() };
        for msg in responses(&key, id, &result, MAX_PART_BYTES) {
            mgr.publisher.producer.send_raw(TOPIC_BROWSE_RESPONSE, topic_key, &msg);
        }
    }
}

/// Pesan respons (satu atau lebih bagian, tiap bagian <= `limit` byte objek) sebagai JSON.
fn responses(key: &str, id_rtu: Option<i32>, result: &Result<BrowseResult, String>, limit: usize) -> Vec<Value> {
    let empty = BrowseResult::default();
    let (status, message, r) = match result {
        Ok(r) => {
            let mut m = format!("{} objek", r.objects.len());
            if !r.errors.is_empty() {
                m.push_str(&format!(", {} node gagal (lihat errors)", r.errors.len()));
            }
            if !r.complete {
                m.push_str(", belum lengkap (BROWSE_TIMEOUT_MS)");
            }
            ("SUCCESS", m, r)
        }
        Err(e) => {
            warn!("[BROWSE] id_rtu={id_rtu:?} gagal: {e}");
            ("FAILED", e.clone(), &empty)
        }
    };
    let complete = (status == "SUCCESS" && !r.complete).then_some(false);
    let parts = split_parts(&r.objects, limit);
    let total_parts = parts.len();
    parts
        .iter()
        .enumerate()
        .map(|(i, objects)| {
            let first = i == 0;
            let resp = BrowseResponse {
                key,
                id_rtu,
                status,
                message: &message,
                objects,
                part: i + 1,
                total_parts,
                errors: if first { r.errors.as_slice() } else { &[] },
                rcbs: if first { r.rcbs.as_slice() } else { &[] },
                complete,
            };
            serde_json::to_value(resp).unwrap_or(Value::Null)
        })
        .collect()
}

/// Bagi objek supaya JSON tiap bagian <= `limit` byte (perkiraan per objek). Minimal satu bagian.
pub fn split_parts(objects: &[BrowseObject], limit: usize) -> Vec<&[BrowseObject]> {
    let mut parts = Vec::new();
    // 2 = kurung [] array
    let (mut start, mut size) = (0, 2);
    for (i, o) in objects.iter().enumerate() {
        let n = serde_json::to_string(o).map_or(0, |s| s.len()) + 1;
        if size + n > limit && i > start {
            parts.push(&objects[start..i]);
            (start, size) = (i, 2);
        }
        size += n;
    }
    parts.push(&objects[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(i: usize) -> BrowseObject {
        BrowseObject { object_reference: format!("LD/GGIO1.Ind{i}"), fc: "ST".into(), cdc: Some("SPS".into()), control: None }
    }

    fn ok(objects: Vec<BrowseObject>) -> Result<BrowseResult, String> {
        Ok(BrowseResult { objects, complete: true, ..Default::default() })
    }

    #[test]
    fn dibagi_per_ukuran() {
        let objs: Vec<_> = (0..1000).map(obj).collect();
        let parts = split_parts(&objs, 10_000);
        assert!(parts.len() > 1);
        assert_eq!(parts.iter().map(|p| p.len()).sum::<usize>(), 1000);
        for p in &parts {
            assert!(serde_json::to_string(p).unwrap().len() <= 10_000);
        }
        assert_eq!(split_parts(&[], 10).len(), 1); // kosong tetap satu pesan
    }

    #[test]
    fn respons_gagal_dan_sukses() {
        let r = responses("k1", Some(9001), &Err("IED tidak terkoneksi".into()), MAX_PART_BYTES);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["status"], "FAILED");
        assert_eq!(r[0]["message"], "IED tidak terkoneksi");
        assert_eq!(r[0]["objects"], serde_json::json!([]));
        assert_eq!((r[0]["part"].as_u64(), r[0]["total_parts"].as_u64()), (Some(1), Some(1)));
        // field opsional tidak muncul
        assert!(r[0].get("errors").is_none() && r[0].get("rcbs").is_none() && r[0].get("complete").is_none());

        let r = responses("k2", Some(9001), &ok(vec![obj(1)]), MAX_PART_BYTES);
        assert_eq!(r[0]["status"], "SUCCESS");
        assert_eq!(r[0]["objects"][0]["object_reference"], "LD/GGIO1.Ind1");
        assert_eq!(r[0]["objects"][0]["cdc"], "SPS");
        assert!(r[0]["objects"][0].get("control").is_none());
    }

    #[test]
    fn errors_rcbs_hanya_di_bagian_pertama_dan_belum_lengkap() {
        let res = Ok(BrowseResult {
            objects: (0..4).map(obj).collect(),
            errors: vec![BrowseError { reference: "LD/XSWI1[ST]".into(), error: "malformed-message".into() }],
            rcbs: vec![RcbInfo { reference: "LD/LLN0.BR.brcbST01".into(), ..Default::default() }],
            complete: false,
        });
        let r = responses("k3", Some(9302), &res, 160);
        assert!(r.len() > 1);
        assert_eq!(r[0]["errors"][0]["ref"], "LD/XSWI1[ST]");
        assert_eq!(r[0]["rcbs"][0]["reference"], "LD/LLN0.BR.brcbST01");
        assert!(r[1].get("errors").is_none() && r[1].get("rcbs").is_none());
        assert!(r.iter().all(|p| p["complete"] == false));
        assert_eq!(r[0]["message"], "4 objek, 1 node gagal (lihat errors), belum lengkap (BROWSE_TIMEOUT_MS)");
    }

    fn field<T: serde::de::DeserializeOwned>(v: &Value, k: &str) -> Vec<T> {
        v.get(k).cloned().map_or_else(Vec::new, |x| serde_json::from_value(x).unwrap())
    }

    /// Contoh IEC61850_BROWSE_RESPONSE di docs/examples/produce = keluaran responses().
    #[test]
    fn contoh_dokumen_browse() {
        use crate::kafka::payload_examples::{examples_of, int_of, str_of};
        let ex: std::collections::BTreeMap<String, Value> = examples_of("produce", TOPIC_BROWSE_RESPONSE).into_iter().collect();
        let get = |n: &str| ex.get(&format!("{TOPIC_BROWSE_RESPONSE}__{n}")).cloned().unwrap_or_else(|| panic!("contoh {n}"));
        let result = |v: &Value| -> Result<BrowseResult, String> {
            if str_of(v, "status") == "FAILED" {
                return Err(str_of(v, "message").into());
            }
            let complete = v.get("complete").and_then(Value::as_bool).unwrap_or(true);
            Ok(BrowseResult { objects: field(v, "objects"), errors: field(v, "errors"), rcbs: field(v, "rcbs"), complete })
        };
        // semua contoh satu bagian
        for (name, v) in ex.iter().filter(|(_, v)| v["total_parts"] == 1) {
            let id = Some(int_of(v, "id_rtu") as i32);
            assert_eq!(responses(str_of(v, "key"), id, &result(v), MAX_PART_BYTES), vec![v.clone()], "{name}");
        }
        // multi-part dengan batas kecil supaya contoh pendek (FEP memakai MAX_PART_BYTES)
        let (p1, p2) = (get("multipart_1"), get("multipart_2"));
        let all: Vec<BrowseObject> = field(&p1, "objects").into_iter().chain(field(&p2, "objects")).collect();
        assert_eq!(responses("browse-9001-02", Some(9001), &ok(all), 160), vec![p1, p2]);
    }

    #[tokio::test]
    async fn id_rtu_dicari_dari_ip() {
        let mgr = crate::connection::testing::manager();
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let cfg = crate::domain::RtuConfig { ip_address: "10.0.0.91".into(), ..crate::connection::testing::rtu(9001) };
        crate::connection::testing::fake_ied_cfg(&mgr, cfg, move |rx| {
            while let Ok(ctl) = rx.recv() {
                match ctl {
                    Ctl::Browse(reply) => {
                        let _ = seen_tx.send(());
                        let _ = reply.send(ok(vec![]));
                    }
                    Ctl::Stop => break,
                    _ => {}
                }
            }
        });
        for json in [
            r#"{"event":"BROWSE","protocol":"IEC61850","ip_address":"10.0.0.91","key":"k1"}"#,
            r#"{"event":"BROWSE","protocol":"IEC61850","data":{"ip_address":"10.0.0.91","key":"k2"}}"#,
        ] {
            let m: RtuEventMessage = serde_json::from_str(json).unwrap();
            tokio::time::timeout(Duration::from_secs(5), submit(&mgr, m)).await.expect(json);
            assert!(seen_rx.recv_timeout(Duration::from_secs(1)).is_ok(), "{json}");
        }
        mgr.stop_all(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn ied_tidak_berjalan_langsung_failed() {
        let mgr = crate::connection::testing::manager();
        let m: RtuEventMessage =
            serde_json::from_str(r#"{"event":"BROWSE","protocol":"IEC61850","id_rtu":4242,"key":"k"}"#).unwrap();
        // tidak ada thread IED 4242 -> selesai tanpa menunggu timeout
        tokio::time::timeout(Duration::from_secs(5), submit(&mgr, m)).await.unwrap();
    }
}
