//! Event COMMAND (RTU_EVENTS) -> select / operate / cancel IEC 61850 -> respons RTU_COMMAND.
//! Format payload & respons sama dengan `kafka/handlers/rtu_event/command` FEP IEC 104.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::connection::Manager;
use crate::domain::{CommandAction, CommandJob, CommandReq};
use crate::iec61850::acquisition::Ctl;
use crate::kafka::event_handler::data_id_rtu;
use crate::kafka::models::{RtuEventMessage, PROTOCOL};
use crate::utils::timestamp::now_ms;

const TOPIC_RTU_COMMAND: &str = "RTU_COMMAND";
/// Port MMS standar, untuk respons IED tidak dikenal tanpa `port` di payload.
const DEFAULT_MMS_PORT: i32 = 102;
/// Batas tunggu hasil command dari thread IED (termasuk CommandTermination).
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// IOA dari akhiran tag `NAMA_123` (sama dengan regex `_(\d+)$` di IEC 104).
pub fn ioa_from_tag(tag: &str) -> Option<i32> {
    let (_, digits) = tag.rsplit_once('_')?;
    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.parse().ok())?
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CommandData {
    action: String,
    tag: Option<String>,
    ip_address: String,
    /// Hanya untuk respons kalau IED tidak dikenal; IED dikenal -> port konfigurasi
    port: Option<u16>,
    value: Option<Value>,
    #[serde(rename = "type")]
    command_type: Option<String>,
    id_rtu: Option<i32>,
    key: Option<String>,
    ioa: Option<Value>,
    user: Option<String>,
    /// Dikembalikan apa adanya di respons (default 0)
    qualifier: Option<u8>,
}

fn port_of(p: Option<i32>) -> Option<u16> {
    p.and_then(|p| u16::try_from(p).ok())
}

fn as_i32(v: Option<&Value>) -> Option<i32> {
    match v? {
        Value::Number(n) => n.as_i64().map(|v| v as i32),
        Value::String(s) => s.trim().parse().ok(),
        Value::Bool(b) => Some(*b as i32),
        _ => None,
    }
}

/// value payload: angka (bulat/float), string angka, atau bool (kompatibel payload lama).
fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        Value::Bool(b) => Some(f64::from(u8::from(*b))),
        _ => None,
    }
}

/// command_value di respons: bilangan bulat tetap dikirim sebagai integer (format lama).
fn value_json(v: Option<f64>) -> Value {
    match v {
        Some(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => Value::from(f as i64),
        Some(f) => Value::from(f),
        None => Value::Null,
    }
}

/// Utamakan objek `data`; kalau tidak ada, pakai field datar (sama dengan IEC 104).
/// `data` ada tapi salah tipe -> Err (TIDAK jatuh diam-diam ke field datar).
fn parse_command_data(m: &RtuEventMessage) -> Result<CommandData, String> {
    if let Some(d) = m.data.as_ref().filter(|d| d.is_object()) {
        let mut cd = CommandData::deserialize(d).map_err(|e| format!("field data tidak valid: {e}"))?;
        if cd.ip_address.is_empty() {
            cd.ip_address = m.ip_address.clone();
        }
        if cd.id_rtu.is_none() {
            cd.id_rtu = m.id_rtu;
        }
        if cd.user.is_none() {
            cd.user = m.user.clone();
        }
        if cd.key.is_none() {
            cd.key = m.key.clone();
        }
        if cd.port.is_none() {
            cd.port = port_of(m.port);
        }
        if cd.qualifier.is_none() {
            cd.qualifier = m.qualifier;
        }
        return Ok(cd);
    }
    Ok(CommandData {
        action: m.action.clone().unwrap_or_default(),
        tag: m.tag.clone(),
        ip_address: m.ip_address.clone(),
        port: port_of(m.port),
        value: m.value.clone(),
        command_type: m.command_type.clone(),
        id_rtu: m.id_rtu,
        key: m.key.clone(),
        ioa: m.ioa.clone().map(Value::String),
        user: m.user.clone(),
        qualifier: m.qualifier,
    })
}

/// Field yang masih terbaca dari payload yang gagal di-parse, hanya untuk isi respons.
fn lenient(m: &RtuEventMessage) -> CommandData {
    let d = m.data.as_ref();
    let s = |k: &str| d.and_then(|d| d.get(k)).and_then(Value::as_str).map(String::from);
    CommandData {
        action: s("action").or_else(|| m.action.clone()).unwrap_or_default(),
        tag: s("tag").or_else(|| m.tag.clone()),
        ip_address: s("ip_address").unwrap_or_else(|| m.ip_address.clone()),
        command_type: s("type").or_else(|| m.command_type.clone()),
        id_rtu: data_id_rtu(d).or(m.id_rtu),
        key: s("key").or_else(|| m.key.clone()),
        value: d.and_then(|d| d.get("value")).cloned().or_else(|| m.value.clone()),
        port: d.and_then(|d| d.get("port")).and_then(Value::as_i64).and_then(|p| u16::try_from(p).ok()).or(port_of(m.port)),
        qualifier: d.and_then(|d| d.get("qualifier")).and_then(Value::as_u64).and_then(|q| u8::try_from(q).ok()).or(m.qualifier),
        ..Default::default()
    }
}

/// `event` respons: SELECT/OPERATE/CANCEL; action tidak dikenal -> teksnya tanpa `_RTU` (kosong: UNKNOWN).
fn event_name(action: &str) -> String {
    match CommandAction::parse(action) {
        Some(a) => a.as_str().into(),
        None if action.trim().is_empty() => "UNKNOWN".into(),
        None => {
            let a = action.trim().to_uppercase();
            a.strip_suffix("_RTU").map_or_else(|| a.clone(), String::from)
        }
    }
}

/// Bagian SINKRON: parse lalu masukkan command ke antrean thread IED saat itu juga, jadi urutan
/// command ke satu IED = urutan kedatangan Kafka (SELECT selalu sebelum OPERATE).
/// Future yang dikembalikan (tunggu hasil + kirim respons RTU_COMMAND) boleh di-`tokio::spawn`.
/// Semua kegagalan sebelum eksekusi (field salah tipe, action tidak dikenal, IED tidak ada) juga dibalas
/// RTU_COMMAND `inactive`.
pub fn submit(mgr: &Arc<Manager>, m: RtuEventMessage) -> impl Future<Output = ()> + Send + 'static {
    let (cd, parsed) = match parse_command_data(&m) {
        Ok(cd) => (cd, Ok(())),
        Err(e) => (lenient(&m), Err(e)),
    };
    let event = event_name(&cd.action);
    let value = as_f64(cd.value.as_ref());
    let ioa = as_i32(cd.ioa.as_ref()).or_else(|| cd.tag.as_deref().and_then(ioa_from_tag));
    info!("[COMMAND] {event} ip={} id_rtu={:?} ioa={ioa:?} value={value:?} key={:?}", cd.ip_address, cd.id_rtu, cd.key);

    let queued = parsed.and_then(|()| match CommandAction::parse(&cd.action) {
        Some(action) => enqueue(mgr, &cd, action, ioa, value),
        None => {
            warn!("[COMMAND] action tidak dikenal: {:?}", cd.action);
            Err(format!("action tidak dikenal: {:?} (SELECT_RTU, OPERATE_RTU, CANCEL_RTU)", cd.action))
        }
    });
    let mgr = Arc::clone(mgr);
    async move {
        let result = match queued {
            Ok(rx) => match tokio::time::timeout(COMMAND_TIMEOUT, rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("thread IED berhenti sebelum command selesai".into()),
                Err(_) => Err("timeout menunggu hasil command".into()),
            },
            Err(e) => Err(e),
        };
        respond(&mgr, &cd, &event, ioa, value, &result);
    }
}

/// COMMAND IEC61850 yang JSON-nya tidak terbaca struct (dari consumer): dibalas inactive.
pub fn reject(mgr: &Manager, action: &str, key: String, ip_address: String, error: &str) {
    warn!("[COMMAND] payload tidak valid key={key:?}: {error}");
    let cd = CommandData { action: action.into(), ip_address, key: Some(key), ..Default::default() };
    respond(mgr, &cd, &event_name(action), None, None, &Err(error.to_string()));
}

fn respond(mgr: &Manager, cd: &CommandData, event: &str, ioa: Option<i32>, value: Option<f64>, result: &Result<(), String>) {
    match result {
        Ok(()) => info!("[COMMAND] {event} sukses ioa={ioa:?}"),
        Err(e) => error!("[COMMAND] {event} gagal ioa={ioa:?}: {e}"),
    }
    let cfg = cd.id_rtu.or_else(|| mgr.id_by_ip(&cd.ip_address)).and_then(|id| mgr.endpoint_of(id));
    let response = response_json(cd, event, value, result, response_endpoint(&cd.ip_address, cd.port, cfg), now_ms());
    let key = cd.key.as_deref().unwrap_or_default();
    let topic_key = if key.is_empty() { "unknown" } else { key };
    mgr.publisher.producer.send_raw(TOPIC_RTU_COMMAND, topic_key, &response);
}

/// Isi pesan RTU_COMMAND (format sama dengan IEC 104). `endpoint` = (hostname, port).
fn response_json(
    cd: &CommandData,
    event: &str,
    value: Option<f64>,
    result: &Result<(), String>,
    (hostname, port): (String, i32),
    time: i64,
) -> Value {
    let (status, msg) = match result {
        Ok(()) => ("active", format!("{event} success RTU status is Active")),
        Err(e) => ("inactive", format!("{event} failed: {e} - RTU status is Inactive")),
    };
    json!({
        "hostname": hostname,
        "port": port,
        "protocol": PROTOCOL,
        "tag": cd.tag.clone().unwrap_or_default(),
        "command_type": cd.command_type.clone().unwrap_or_default(),
        "command_value": value_json(value),
        "qualifier": cd.qualifier.unwrap_or(0),
        "event": event,
        "status": status,
        "msg": msg,
        "key": cd.key.clone().unwrap_or_default(),
        "time": time,
    })
}

/// hostname/port respons: port dari konfigurasi IED (payload sering tanpa port),
/// hostname dari payload, konfigurasi kalau payload kosong. IED tidak dikenal -> isi payload.
fn response_endpoint(ip: &str, port: Option<u16>, cfg: Option<(String, i32)>) -> (String, i32) {
    match cfg {
        Some((cfg_ip, cfg_port)) => (if ip.is_empty() { cfg_ip } else { ip.to_string() }, cfg_port),
        None => (ip.to_string(), port.map_or(DEFAULT_MMS_PORT, i32::from)),
    }
}

/// Validasi + kirim ke antrean thread IED (tanpa menunggu).
fn enqueue(
    mgr: &Manager,
    cd: &CommandData,
    action: CommandAction,
    ioa: Option<i32>,
    value: Option<f64>,
) -> Result<oneshot::Receiver<Result<(), String>>, String> {
    let id = cd.id_rtu.or_else(|| mgr.id_by_ip(&cd.ip_address)).ok_or("RTU tidak ditemukan untuk IP ini")?;
    let ioa = ioa.ok_or("ioa atau tag dengan IOA wajib diisi")?;
    if action != CommandAction::Cancel && value.is_none() {
        return Err("value wajib diisi".into());
    }
    let req = CommandReq {
        action,
        ioa,
        value,
        or_ident: cd.user.clone().filter(|u| !u.is_empty()).unwrap_or_else(|| "FEP-IEC61850".into()),
    };
    let (tx, rx) = oneshot::channel();
    if !mgr.send(id, Ctl::Command(Box::new(CommandJob { req, reply: tx }))) {
        return Err(format!("RTU {id} tidak berjalan"));
    }
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ioa_dari_tag() {
        assert_eq!(ioa_from_tag("TAG_NAME_6000"), Some(6000));
        assert_eq!(ioa_from_tag("SIM1_SPCSO1_CO"), None);
        assert_eq!(ioa_from_tag("NO_IOA"), None);
        assert_eq!(ioa_from_tag("X_"), None);
    }

    #[test]
    fn hostname_port_respons() {
        let cfg = Some(("10.0.0.5".to_string(), 10102));
        assert_eq!(response_endpoint("", None, cfg.clone()), ("10.0.0.5".into(), 10102));
        assert_eq!(response_endpoint("ied-a", None, cfg.clone()), ("ied-a".into(), 10102));
        assert_eq!(response_endpoint("ied-a", Some(9999), cfg), ("ied-a".into(), 10102)); // konfigurasi menang
        assert_eq!(response_endpoint("ied-x", Some(103), None), ("ied-x".into(), 103)); // IED tidak dikenal
        assert_eq!(response_endpoint("ied-x", None, None), ("ied-x".into(), 102));
    }

    #[test]
    fn parse_payload_nested_dan_datar() {
        let nested: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-sim-1",
                "data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"type":"SPC","id_rtu":9001,"key":"k1","user":"op1"}}"#,
        )
        .unwrap();
        let cd = parse_command_data(&nested).unwrap();
        assert_eq!(cd.action, "OPERATE_RTU");
        assert_eq!(cd.ip_address, "ied-sim-1");
        assert_eq!(as_i32(cd.ioa.as_ref()), Some(3001));
        assert_eq!(as_f64(cd.value.as_ref()), Some(1.0));
        assert_eq!(cd.user.as_deref(), Some("op1"));

        let flat: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-sim-1",
                "action":"SELECT_RTU","ioa":"3001","value":"1","id_rtu":9001,"key":"k2"}"#,
        )
        .unwrap();
        let cd = parse_command_data(&flat).unwrap();
        assert_eq!(cd.action, "SELECT_RTU");
        assert_eq!(as_i32(cd.ioa.as_ref()), Some(3001));
        assert_eq!(as_f64(cd.value.as_ref()), Some(1.0));
    }

    /// Contoh COMMAND di docs/examples/consume terbaca utuh (bukan jatuh diam-diam ke field datar).
    #[test]
    fn contoh_dokumen_command() {
        use crate::kafka::payload_examples::examples_of;
        let exs: Vec<_> = examples_of("consume", "RTU_EVENTS").into_iter().filter(|(n, _)| n.contains("__COMMAND")).collect();
        assert!(!exs.is_empty());
        for (name, v) in &exs {
            let m: RtuEventMessage = serde_json::from_value(v.clone()).unwrap();
            if let Some(d) = m.data.as_ref() {
                CommandData::deserialize(d).unwrap_or_else(|e| panic!("{name}: data: {e}"));
            }
            let cd = parse_command_data(&m).unwrap_or_else(|e| panic!("{name}: {e}"));
            let action = CommandAction::parse(&cd.action).unwrap_or_else(|| panic!("{name}: action {:?}", cd.action));
            assert!(as_i32(cd.ioa.as_ref()).or_else(|| cd.tag.as_deref().and_then(ioa_from_tag)).is_some(), "{name}: ioa");
            assert_eq!(as_f64(cd.value.as_ref()).is_some(), action != CommandAction::Cancel, "{name}: value");
            assert!(cd.id_rtu.is_some() || !cd.ip_address.is_empty(), "{name}: id_rtu / ip_address");
        }
        // compat104 juga terbaca bentuk FEP IEC 104: ip_address atas wajib, data.ip_address/port wajib, value i32, type wajib
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Iec104Event {
            ip_address: String,
            data: Iec104Data,
        }
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Iec104Data {
            action: String,
            ip_address: String,
            port: u16,
            value: i32,
            #[serde(rename = "type")]
            command_type: String,
            key: String,
        }
        let (_, v) = exs.iter().find(|(n, _)| n.ends_with("COMMAND.compat104")).expect("contoh compat104");
        let e: Iec104Event = serde_json::from_value(v.clone()).unwrap();
        assert!(!e.ip_address.is_empty() && !e.data.ip_address.is_empty() && e.data.port > 0);
    }

    /// Contoh RTU_COMMAND di docs/examples/produce = keluaran response_json.
    #[test]
    fn contoh_dokumen_rtu_command() {
        use crate::kafka::payload_examples::{examples_of, int_of, str_of};
        let exs = examples_of("produce", TOPIC_RTU_COMMAND);
        assert!(!exs.is_empty());
        for (name, v) in exs {
            let event = str_of(&v, "event");
            let msg = str_of(&v, "msg");
            let result = match str_of(&v, "status") {
                "active" => Ok(()),
                "inactive" => Err(msg
                    .strip_prefix(&format!("{event} failed: "))
                    .and_then(|m| m.strip_suffix(" - RTU status is Inactive"))
                    .unwrap_or_else(|| panic!("{name}: msg {msg}"))
                    .to_string()),
                s => panic!("{name}: status {s}"),
            };
            let cd = CommandData {
                tag: Some(str_of(&v, "tag").into()),
                command_type: Some(str_of(&v, "command_type").into()),
                key: Some(str_of(&v, "key").into()),
                qualifier: Some(int_of(&v, "qualifier") as u8),
                ..Default::default()
            };
            let endpoint = (str_of(&v, "hostname").to_string(), int_of(&v, "port") as i32);
            let out = response_json(&cd, event, v["command_value"].as_f64(), &result, endpoint, int_of(&v, "time"));
            assert_eq!(out, v, "{name}");
        }
    }

    #[test]
    fn data_salah_tipe_tidak_jatuh_ke_field_datar() {
        let m: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","action":"OPERATE_RTU","ioa":3001,"value":1,
                "data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"port":"102","key":"k9"}}"#,
        )
        .unwrap();
        let e = parse_command_data(&m).err().expect("harus gagal");
        assert!(e.starts_with("field data tidak valid"), "{e}");
        // respons tetap membawa key / action dari data
        let cd = lenient(&m);
        assert_eq!((cd.key.as_deref(), event_name(&cd.action).as_str()), (Some("k9"), "OPERATE"));
    }

    #[test]
    fn qualifier_dan_port_dari_payload() {
        let m: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","port":10102,"qualifier":3,
                "data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"id_rtu":9001}}"#,
        )
        .unwrap();
        let cd = parse_command_data(&m).unwrap();
        assert_eq!((cd.port, cd.qualifier), (Some(10102), Some(3)));
        let r = response_json(&cd, "OPERATE", Some(1.0), &Ok(()), ("h".into(), 102), 0);
        assert_eq!(r["qualifier"], 3);
        // data menang atas tingkat atas; tanpa qualifier -> 0
        let m: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","qualifier":3,"data":{"action":"OPERATE_RTU","qualifier":1,"port":103}}"#,
        )
        .unwrap();
        let cd = parse_command_data(&m).unwrap();
        assert_eq!((cd.port, cd.qualifier), (Some(103), Some(1)));
        let cd = CommandData::default();
        assert_eq!(response_json(&cd, "OPERATE", None, &Ok(()), ("h".into(), 102), 0)["qualifier"], 0);
    }

    #[test]
    fn nama_event_respons() {
        assert_eq!(event_name("select_rtu"), "SELECT");
        assert_eq!(event_name("CANCEL_RTU"), "CANCEL");
        assert_eq!(event_name("TRIP_RTU"), "TRIP");
        assert_eq!(event_name("open"), "OPEN");
        assert_eq!(event_name(" "), "UNKNOWN");
    }

    /// Gagal sebelum eksekusi -> respons langsung (tanpa menunggu COMMAND_TIMEOUT).
    #[tokio::test]
    async fn gagal_sebelum_eksekusi_tetap_dibalas() {
        let mgr = crate::connection::testing::manager();
        for json in [
            // action tidak dikenal
            r#"{"event":"COMMAND","protocol":"IEC61850","data":{"action":"TRIP_RTU","ioa":3001,"value":1,"id_rtu":9001,"key":"a"}}"#,
            // field data salah tipe
            r#"{"event":"COMMAND","protocol":"IEC61850","data":{"action":"OPERATE_RTU","id_rtu":[1],"key":"b"}}"#,
            // IED tidak berjalan
            r#"{"event":"COMMAND","protocol":"IEC61850","data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"id_rtu":4242,"key":"c"}}"#,
        ] {
            let m: RtuEventMessage = serde_json::from_str(json).unwrap();
            tokio::time::timeout(Duration::from_secs(2), submit(&mgr, m)).await.expect(json);
        }
        reject(&mgr, "OPERATE_RTU", "d".into(), String::new(), "payload tidak valid");
    }

    #[test]
    fn value_float_dan_lama() {
        use serde_json::json;
        assert_eq!(as_f64(Some(&json!(2))), Some(2.0));
        assert_eq!(as_f64(Some(&json!(12.5))), Some(12.5));
        assert_eq!(as_f64(Some(&json!(" 3.25 "))), Some(3.25));
        assert_eq!(as_f64(Some(&json!("2"))), Some(2.0));
        assert_eq!(as_f64(Some(&json!(true))), Some(1.0));
        assert_eq!(as_f64(Some(&json!("x"))), None);
        assert_eq!(value_json(Some(2.0)), json!(2)); // tetap integer di respons
        assert_eq!(value_json(Some(12.5)), json!(12.5));
        assert_eq!(value_json(None), Value::Null);
    }
}
