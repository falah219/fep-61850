//! Event BROWSE (RTU_EVENTS): browse model IED di thread IED -> IEC61850_BROWSE_RESPONSE.
//! IED besar bisa ribuan objek -> dikirim per bagian (`part` 1..=`total_parts`) maks ~900 KB.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::connection::Manager;
use crate::iec61850::acquisition::Ctl;
use crate::iec61850::browse::BrowseObject;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::RtuEventMessage;
use crate::kafka::TOPIC_BROWSE_RESPONSE;

/// Browse = beberapa request MMS per LN; IED besar bisa butuh lama.
const BROWSE_TIMEOUT: Duration = Duration::from_secs(300);
/// Di bawah batas default Kafka 1 MB (message.max.bytes), sisa untuk field lain.
const MAX_PART_BYTES: usize = 900 * 1024;

#[derive(Debug, Serialize)]
struct BrowseResponse<'a> {
    key: &'a str,
    id_rtu: Option<i32>,
    status: &'static str,
    message: &'a str,
    objects: &'a [BrowseObject],
    part: usize,
    total_parts: usize,
}

/// Bagian SINKRON: kirim ke antrean thread IED. Future (tunggu + kirim respons) boleh di-spawn.
pub fn submit(mgr: &Arc<Manager>, m: RtuEventMessage) -> impl Future<Output = ()> + Send + 'static {
    let data = m.data.as_ref();
    let id = m.id_rtu.or_else(|| data_id_rtu(data));
    let key = m.key.clone().or_else(|| data.and_then(|d| d.get("key")).and_then(Value::as_str).map(String::from)).unwrap_or_default();
    info!("[BROWSE] id_rtu={id:?} key={key:?}");
    let queued = match id {
        None => Err("id_rtu wajib diisi".to_string()),
        Some(id) => {
            let (tx, rx) = oneshot::channel();
            if mgr.send(id, Ctl::Browse(tx)) { Ok(rx) } else { Err("IED tidak terkoneksi".into()) }
        }
    };
    let mgr = Arc::clone(mgr);
    async move {
        let result = match queued {
            Ok(rx) => match tokio::time::timeout(BROWSE_TIMEOUT, rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => Err("thread IED berhenti sebelum browse selesai".into()),
                Err(_) => Err("timeout menunggu hasil browse".into()),
            },
            Err(e) => Err(e),
        };
        let topic_key = if key.is_empty() { "unknown" } else { key.as_str() };
        for msg in responses(&key, id, &result) {
            mgr.publisher.producer.send_raw(TOPIC_BROWSE_RESPONSE, topic_key, &msg);
        }
    }
}

/// Pesan respons (satu atau lebih bagian) sebagai JSON.
fn responses(key: &str, id_rtu: Option<i32>, result: &Result<Vec<BrowseObject>, String>) -> Vec<Value> {
    let (status, message, objects): (_, String, &[BrowseObject]) = match result {
        Ok(o) => ("SUCCESS", format!("{} objek", o.len()), o),
        Err(e) => {
            warn!("[BROWSE] id_rtu={id_rtu:?} gagal: {e}");
            ("FAILED", e.clone(), &[])
        }
    };
    let parts = split_parts(objects, MAX_PART_BYTES);
    let total_parts = parts.len();
    parts
        .iter()
        .enumerate()
        .map(|(i, objects)| {
            let r = BrowseResponse { key, id_rtu, status, message: &message, objects, part: i + 1, total_parts };
            serde_json::to_value(r).unwrap_or(Value::Null)
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
        BrowseObject { object_reference: format!("LD/GGIO1.Ind{i}"), fc: "ST".into(), cdc: Some("SPS".into()) }
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
        let r = responses("k1", Some(9001), &Err("IED tidak terkoneksi".into()));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["status"], "FAILED");
        assert_eq!(r[0]["message"], "IED tidak terkoneksi");
        assert_eq!(r[0]["objects"], serde_json::json!([]));
        assert_eq!((r[0]["part"].as_u64(), r[0]["total_parts"].as_u64()), (Some(1), Some(1)));

        let r = responses("k2", Some(9001), &Ok(vec![obj(1)]));
        assert_eq!(r[0]["status"], "SUCCESS");
        assert_eq!(r[0]["objects"][0]["object_reference"], "LD/GGIO1.Ind1");
        assert_eq!(r[0]["objects"][0]["cdc"], "SPS");
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
