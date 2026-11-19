//! Event BROWSE (RTU_EVENTS): browse model IED di thread IED -> IEC61850_BROWSE_RESPONSE.
//! IED besar bisa ribuan objek -> dikirim per bagian (`part` 1..=`total_parts`) maks ~900 KB.
//! `responses` juga dipakai SCL_IMPORT (`scl_import.rs`, `source: "SCL"` + field SCL; BROWSE: `source: "BROWSE"`).
//! Skema stabil untuk GRITA: semua field selalu ada, urutan JSON tetap (struct diserialisasi langsung,
//! bukan lewat `serde_json::Value` yang mengurutkan key). `rcbs` / `errors` lengkap di bagian 1, `[]` di bagian lain.

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
use crate::scl::{ConfigRev, IedInfo, IedRef};

/// Di atas BROWSE_TIMEOUT_MS: thread IED membalas hasil sebagian sendiri saat batas waktunya tercapai.
const REPLY_MARGIN: Duration = Duration::from_secs(30);
/// Di bawah batas default Kafka 1 MB (message.max.bytes), sisa untuk field lain.
pub(super) const MAX_PART_BYTES: usize = 900 * 1024;

/// Field tambahan respons SCL_IMPORT. Semua tidak dikirim kalau None -> respons BROWSE tidak berubah.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(super) struct SclMeta<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id_scl_file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ied: Option<&'a IedInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_revs: Option<&'a [ConfigRev]>,
    /// FAILED "Pilih IED" / IED tidak ada: pilihan IED di file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ied_names: Option<&'a [IedRef]>,
}

/// Satu pesan IEC61850_BROWSE_RESPONSE. Urutan field = urutan JSON.
#[derive(Debug, Serialize)]
pub(super) struct BrowseResponse<'a> {
    key: &'a str,
    id_rtu: Option<i32>,
    status: &'static str,
    /// `BROWSE` (dari IED) / `SCL` (SCL_IMPORT)
    source: &'static str,
    #[serde(flatten)]
    scl: SclMeta<'a>,
    message: String,
    objects: &'a [BrowseObject],
    /// RCB + anggota dataset (bagian 1; bagian lain `[]`)
    rcbs: &'a [RcbInfo],
    /// Error nyata (bagian 1; bagian lain `[]`). FAILED: penyebabnya
    errors: Vec<BrowseError>,
    part: usize,
    total_parts: usize,
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
        for msg in responses(&key, id, &result, None, MAX_PART_BYTES) {
            mgr.publisher.producer.send_raw(TOPIC_BROWSE_RESPONSE, topic_key, &msg);
        }
    }
}

/// `<n> objek, <m> RCB[, <k> error]`.
fn success_message(r: &BrowseResult) -> String {
    let mut m = format!("{} objek, {} RCB", r.objects.len(), r.rcbs.len());
    if !r.errors.is_empty() {
        m.push_str(&format!(", {} error", r.errors.len()));
    }
    m
}

/// Pesan respons (satu atau lebih bagian, tiap bagian <= `limit` byte objek). `scl` = Some untuk SCL_IMPORT.
pub(super) fn responses<'a>(
    key: &'a str,
    id_rtu: Option<i32>,
    result: &'a Result<BrowseResult, String>,
    scl: Option<SclMeta<'a>>,
    limit: usize,
) -> Vec<BrowseResponse<'a>> {
    let source = if scl.is_some() { "SCL" } else { "BROWSE" };
    let scl = scl.unwrap_or_default();
    let r = match result {
        Ok(r) => r,
        Err(e) => {
            warn!("[{source}] id_rtu={id_rtu:?} gagal: {e}");
            let errors = vec![BrowseError { reference: source.to_lowercase(), error: e.clone() }];
            return vec![BrowseResponse {
                key,
                id_rtu,
                status: "FAILED",
                source,
                scl,
                message: e.clone(),
                objects: &[],
                rcbs: &[],
                errors,
                part: 1,
                total_parts: 1,
            }];
        }
    };
    let message = if source == "SCL" { format!("Import SCL selesai: {}", success_message(r)) } else { success_message(r) };
    let parts = split_parts(&r.objects, limit);
    let total_parts = parts.len();
    parts
        .into_iter()
        .enumerate()
        .map(|(i, objects)| {
            let first = i == 0;
            BrowseResponse {
                key,
                id_rtu,
                status: "SUCCESS",
                source,
                scl,
                message: message.clone(),
                objects,
                rcbs: if first { r.rcbs.as_slice() } else { &[] },
                errors: if first { r.errors.clone() } else { Vec::new() },
                part: i + 1,
                total_parts,
            }
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
    use crate::iec61850::browse::{ControlInfo, DatasetMember};

    fn obj(i: usize) -> BrowseObject {
        BrowseObject { object_reference: format!("LD/GGIO1.Ind{i}"), fc: "ST".into(), cdc: Some("SPS".into()), control: None }
    }

    fn ok(objects: Vec<BrowseObject>) -> Result<BrowseResult, String> {
        Ok(BrowseResult { objects, ..Default::default() })
    }

    fn json(r: Vec<BrowseResponse>) -> Vec<Value> {
        r.iter().map(|x| serde_json::to_value(x).unwrap()).collect()
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
    fn skema_stabil_gagal_dan_sukses() {
        let failed = Err("IED tidak terkoneksi".to_string());
        let r = json(responses("k1", Some(9001), &failed, None, MAX_PART_BYTES));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["status"], "FAILED");
        assert_eq!(r[0]["source"], "BROWSE");
        assert!(r[0].get("ied").is_none() && r[0].get("id_scl_file").is_none(), "field SCL tidak ikut di BROWSE");
        assert_eq!(r[0]["message"], "IED tidak terkoneksi");
        assert_eq!((&r[0]["objects"], &r[0]["rcbs"]), (&serde_json::json!([]), &serde_json::json!([])));
        assert_eq!(r[0]["errors"], serde_json::json!([{"ref": "browse", "error": "IED tidak terkoneksi"}]));
        assert_eq!((r[0]["part"].as_u64(), r[0]["total_parts"].as_u64()), (Some(1), Some(1)));

        let res = ok(vec![obj(1)]);
        let r = json(responses("k2", Some(9001), &res, None, MAX_PART_BYTES));
        assert_eq!((r[0]["status"].as_str(), r[0]["message"].as_str()), (Some("SUCCESS"), Some("1 objek, 0 RCB")));
        assert_eq!((&r[0]["rcbs"], &r[0]["errors"]), (&serde_json::json!([]), &serde_json::json!([])));
        // control hanya untuk FC CO
        assert!(r[0]["objects"][0].get("control").is_none());
    }

    #[test]
    fn urutan_field_json_tetap() {
        let res = Ok(BrowseResult {
            objects: vec![BrowseObject { control: Some(ControlInfo::default()), fc: "CO".into(), ..obj(1) }],
            rcbs: vec![RcbInfo {
                reference: "LD/LLN0.BR.b".into(),
                members: vec![DatasetMember { object_reference: "LD/X.Pos.stVal".into(), fc: "ST".into() }],
                ..Default::default()
            }],
            errors: vec![BrowseError { reference: "LD/X".into(), error: "e".into() }],
        });
        let s = serde_json::to_string(&responses("k", Some(1), &res, None, MAX_PART_BYTES)[0]).unwrap();
        let order = |keys: &[&str], from: &str| {
            let pos: Vec<usize> = keys.iter().map(|k| from.find(&format!("\"{k}\":")).unwrap_or_else(|| panic!("{k}: {from}"))).collect();
            assert!(pos.windows(2).all(|w| w[0] < w[1]), "urutan {keys:?}: {from}");
        };
        order(&["key", "id_rtu", "status", "source", "message", "objects", "rcbs", "errors", "part", "total_parts"], &s);
        let rcb = &s[s.find("\"rcbs\":").unwrap()..];
        order(&["reference", "buffered", "dataset", "rpt_id", "rpt_ena", "trg_ops", "intg_pd_ms", "members"], rcb);
    }

    #[test]
    fn rcbs_errors_hanya_di_bagian_pertama() {
        let res = Ok(BrowseResult {
            objects: (0..4).map(obj).collect(),
            errors: vec![BrowseError { reference: "LD/LLN0.BR.brcbMX01".into(), error: "timeout".into() }],
            rcbs: vec![RcbInfo { reference: "LD/LLN0.BR.brcbST01".into(), ..Default::default() }],
        });
        let r = json(responses("k3", Some(9302), &res, None, 160));
        assert!(r.len() > 1);
        assert_eq!(r[0]["errors"][0]["ref"], "LD/LLN0.BR.brcbMX01");
        assert_eq!(r[0]["rcbs"][0]["reference"], "LD/LLN0.BR.brcbST01");
        for p in &r[1..] {
            assert_eq!((&p["rcbs"], &p["errors"]), (&serde_json::json!([]), &serde_json::json!([])));
        }
        assert!(r.iter().all(|p| p["message"] == "4 objek, 1 RCB, 1 error"));
    }

    fn field<T: serde::de::DeserializeOwned>(v: &Value, k: &str) -> Vec<T> {
        serde_json::from_value(v[k].clone()).unwrap()
    }

    /// Contoh IEC61850_BROWSE_RESPONSE di docs/examples/produce = keluaran responses().
    #[test]
    fn contoh_dokumen_browse() {
        use crate::kafka::payload_examples::{examples_of, int_of, str_of};
        let ex: std::collections::BTreeMap<String, Value> = examples_of("produce", TOPIC_BROWSE_RESPONSE).into_iter().collect();
        let get = |n: &str| ex.get(&format!("{TOPIC_BROWSE_RESPONSE}__{n}")).cloned().unwrap_or_else(|| panic!("contoh {n}"));
        for n in ["success", "success_with_errors", "failed", "multipart_1", "multipart_2", "scl_success", "scl_failed"] {
            get(n);
        }
        fn opt<T: serde::de::DeserializeOwned>(v: &Value, k: &str) -> Option<T> {
            v.get(k).map(|x| serde_json::from_value(x.clone()).unwrap())
        }
        let result = |v: &Value| -> Result<BrowseResult, String> {
            if str_of(v, "status") == "FAILED" {
                return Err(str_of(v, "message").into());
            }
            Ok(BrowseResult { objects: field(v, "objects"), errors: field(v, "errors"), rcbs: field(v, "rcbs") })
        };
        for (name, v) in ex.iter().filter(|(_, v)| v["total_parts"] == 1) {
            let id = Some(int_of(v, "id_rtu") as i32);
            let res = result(v);
            let (ied, revs, names): (Option<IedInfo>, Option<Vec<ConfigRev>>, Option<Vec<IedRef>>) =
                (opt(v, "ied"), opt(v, "config_revs"), opt(v, "ied_names"));
            let scl = (v["source"] == "SCL").then(|| SclMeta {
                file_name: v["file_name"].as_str(),
                id_scl_file: v["id_scl_file"].as_str(),
                ied: ied.as_ref(),
                config_revs: revs.as_deref(),
                ied_names: names.as_deref(),
            });
            assert_eq!(json(responses(str_of(v, "key"), id, &res, scl, MAX_PART_BYTES)), vec![v.clone()], "{name}");
        }
        // multi-part dengan batas kecil supaya contoh pendek (FEP memakai MAX_PART_BYTES)
        let (p1, p2) = (get("multipart_1"), get("multipart_2"));
        let mut all = result(&p1).unwrap();
        all.objects.extend(field::<BrowseObject>(&p2, "objects"));
        let all = Ok(all);
        assert_eq!(json(responses("browse-9001-02", Some(9001), &all, None, 160)), vec![p1, p2]);
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
