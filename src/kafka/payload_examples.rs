//! Contoh JSON di `docs/examples/` (dicantumkan di `docs/KAFKA_PAYLOADS.md`) harus cocok dengan kode:
//! consume dibaca `consumer::parse` (struct yang dipakai handler), produce sama dengan hasil serialize struct.
//! COMMAND / RTU_COMMAND dan BROWSE dicek di test `rtu_event/command.rs` dan `rtu_event/browse.rs`.

use std::path::Path;

use serde_json::Value;

use super::consumer::{parse, KafkaEvent};
use super::event_handler::is_ours;
use super::models::{DataMessage, ManualSetEventResponse, RtuLoggingMessage, RtuStatusMessage};
use super::{TOPIC_BROWSE_RESPONSE, TOPIC_DATAPOINTS, TOPIC_MANUAL_SET_RESPONSE, TOPIC_RTU_DATAS, TOPIC_RTU_LOGGING, TOPIC_RTU_STATUS};
use crate::domain::CtlVal;
use crate::ffi;
use crate::iec61850::mapper::{action_rank, quality_flag};
use crate::iec61850::traffic::{self, TrafficLog};

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// (nama file tanpa `.json`, isi) semua contoh di `docs/examples/<sub>/`, urut nama.
pub(crate) fn examples(sub: &str) -> Vec<(String, Value)> {
    let dir = root().join("docs/examples").join(sub);
    let mut out: Vec<(String, Value)> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .map(|p| {
            let name = p.file_stem().unwrap().to_string_lossy().into_owned();
            let v = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap_or_else(|e| panic!("{name}: {e}"));
            (name, v)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(!out.is_empty(), "tidak ada contoh di {}", dir.display());
    out
}

/// Satu contoh `docs/examples/<sub>/<name>.json`.
pub(crate) fn example(sub: &str, name: &str) -> Value {
    examples(sub).into_iter().find(|(n, _)| n == name).map(|(_, v)| v).unwrap_or_else(|| panic!("contoh {sub}/{name} tidak ada"))
}

/// Contoh satu topic. Nama file: `TOPIC__varian.json`.
pub(crate) fn examples_of(sub: &str, topic: &str) -> Vec<(String, Value)> {
    examples(sub).into_iter().filter(|(n, _)| topic_of(n) == topic).collect()
}

fn topic_of(name: &str) -> &str {
    name.split("__").next().unwrap_or(name)
}

/// `TOPIC__EVENT.varian` -> `EVENT`
fn variant(name: &str) -> &str {
    name.split("__").nth(1).and_then(|v| v.split('.').next()).unwrap_or("")
}

pub(crate) fn str_of<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or_else(|| panic!("field {k} bukan string: {v}"))
}

pub(crate) fn int_of(v: &Value, k: &str) -> i64 {
    v[k].as_i64().unwrap_or_else(|| panic!("field {k} bukan integer: {v}"))
}

#[test]
fn semua_contoh_ada_di_markdown() {
    let md = std::fs::read_to_string(root().join("docs/KAFKA_PAYLOADS.md")).unwrap().replace("\r\n", "\n");
    let blocks: Vec<Value> = md
        .split("```json\n")
        .skip(1)
        .map(|b| b.split("\n```").next().unwrap_or(""))
        .map(|b| serde_json::from_str(b).unwrap_or_else(|e| panic!("blok json tidak valid di KAFKA_PAYLOADS.md: {e}\n{b}")))
        .collect();
    for sub in ["consume", "produce", "iec104"] {
        for (name, v) in examples(sub) {
            assert!(blocks.contains(&v), "docs/examples/{sub}/{name}.json tidak dicantumkan (atau beda) di KAFKA_PAYLOADS.md");
        }
    }
}

#[test]
fn contoh_consume_terbaca_handler() {
    for (name, v) in examples("consume") {
        let topic = topic_of(&name);
        let event = parse(topic, &v.to_string())
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .unwrap_or_else(|| panic!("{name}: topic {topic} tidak dikenal consumer"));
        let ev = variant(&name);
        let protocol = match &event {
            KafkaEvent::RtuEvent(m) => {
                assert_eq!(m.event, ev, "{name}");
                Some(m.protocol.clone())
            }
            KafkaEvent::MasterDatapoint(m) => {
                assert_eq!(m.event, ev, "{name}");
                Some(m.protocol.clone())
            }
            KafkaEvent::MasterRtu(m) => {
                assert_eq!(m.event, ev, "{name}");
                Some(m.protocol.clone())
            }
            KafkaEvent::ManualSet(m) => {
                assert_eq!(m.event, ev, "{name}");
                Some(m.protocol.clone())
            }
            KafkaEvent::CommandConfig(m) => {
                assert_eq!(m.action, ev, "{name}");
                Some(m.protocol.clone())
            }
            KafkaEvent::CaptureDataChange(m) => {
                assert_eq!((m.schema.as_str(), m.table.as_str()), ("public", ev), "{name}");
                None
            }
            KafkaEvent::MasterDataObject => None,
            KafkaEvent::InvalidCommand { error, .. } => panic!("{name}: contoh tidak terbaca: {error}"),
        };
        if let Some(p) = protocol {
            assert_eq!(is_ours(&p), !name.contains("ignored"), "{name}: protocol {p}");
        }
    }
}

#[test]
fn semua_contoh_produce_dicek() {
    let tested =
        [TOPIC_RTU_DATAS, TOPIC_DATAPOINTS, TOPIC_RTU_STATUS, TOPIC_RTU_LOGGING, TOPIC_MANUAL_SET_RESPONSE, "RTU_COMMAND", TOPIC_BROWSE_RESPONSE];
    for (name, _) in examples("produce") {
        assert!(tested.contains(&topic_of(&name)), "{name}: topic tanpa test");
    }
}

#[test]
fn contoh_rtu_datas_dan_datapoints() {
    // semua nama flag dari kode: semua bit detail + INVALID + CLOCK_NOT_SYNC, ditambah QUESTIONABLE dan GOOD
    let flags = quality_flag(Some(0xFFFE), Some(0x20));
    let known: Vec<&str> = flags.split('|').chain(["QUESTIONABLE", "GOOD"]).collect();
    for topic in [TOPIC_RTU_DATAS, TOPIC_DATAPOINTS] {
        let exs = examples_of("produce", topic);
        assert!(!exs.is_empty(), "{topic}");
        for (name, v) in exs {
            let quality = ["GOOD", "INVALID", "SUBSTITUTED", "QUESTIONABLE"]
                .into_iter()
                .find(|q| *q == str_of(&v, "quality"))
                .unwrap_or_else(|| panic!("{name}: quality"));
            let time_desc =
                ["RTU", "SERVER"].into_iter().find(|t| *t == str_of(&v, "time_desc")).unwrap_or_else(|| panic!("{name}: time_desc"));
            let action = str_of(&v, "action");
            assert!(action_rank(action) > 0, "{name}: action {action}");
            let flag = str_of(&v, "flag");
            assert!(flag.split('|').all(|f| known.contains(&f)), "{name}: flag {flag}");
            assert_eq!(str_of(&v, "tag").is_empty(), topic == TOPIC_RTU_DATAS, "{name}: tag");
            let m = DataMessage {
                ip_address: str_of(&v, "ip_address"),
                protocol: "IEC61850",
                id_rtu: int_of(&v, "id_rtu") as i32,
                action,
                tag: str_of(&v, "tag"),
                value: &v["value"],
                address: int_of(&v, "address") as i32,
                ca: 0,
                asdu: 0,
                typ: str_of(&v, "type"),
                quality,
                flag,
                group: str_of(&v, "group"),
                general_action: v["general_action"].as_i64().map(|c| c as i32),
                time: int_of(&v, "time"),
                time_desc,
            };
            assert_eq!(serde_json::to_value(&m).unwrap(), v, "{name}");
        }
    }
}

/// Sampel asli RTU_DATAS FEP IEC 104: nama & tipe field sama dengan RTU_DATAS FEP IEC 61850.
#[test]
fn rtu_datas_sama_bentuk_dengan_sampel_iec104() {
    let kind = |v: &Value| match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    let shape = |v: &Value| -> Vec<(String, &str)> { v.as_object().unwrap().iter().map(|(k, x)| (k.clone(), kind(x))).collect() };
    let iec104 = example("iec104", "RTU_DATAS__iec104_real");
    assert!(!is_ours(str_of(&iec104, "protocol")));
    assert_eq!(shape(&iec104), shape(&example("produce", "RTU_DATAS__DATA_CHANGE")));
}

#[test]
fn contoh_rtu_status() {
    for (name, v) in examples_of("produce", TOPIC_RTU_STATUS) {
        let channel = v.get("channel").map(|c| c.as_i64().unwrap() as i32);
        let mut m = RtuStatusMessage::new(str_of(&v, "ip_address"), int_of(&v, "id_rtu") as i32, int_of(&v, "status") as i32, channel);
        m.time = int_of(&v, "time");
        assert_eq!(serde_json::to_value(&m).unwrap(), v, "{name}");
    }
}

#[test]
fn contoh_rtu_logging() {
    const RCB: &str = "BAY1LD0/LLN0.BR.brcbST01";
    const DS: &str = "BAY1LD0/LLN0$dsStatus";
    const POS: &str = "BAY1LD0/CSWI1.Pos";
    for (name, v) in examples_of("produce", TOPIC_RTU_LOGGING) {
        let line = str_of(&v, "logging");
        let mut m = RtuLoggingMessage::new(str_of(&v, "ip_address"), int_of(&v, "id_rtu") as i32, line);
        m.time = int_of(&v, "time");
        assert_eq!(serde_json::to_value(&m).unwrap(), v, "{name}");
        // isi baris = keluaran traffic.rs
        let expected = match variant(&name) {
            "traffic_report" => traffic::report_rx(RCB, DS, ffi::IEC61850_REASON_DATA_CHANGE, 2, Some(15), Some(&[0x0a, 0, 0, 0, 0, 0, 0, 0])),
            "traffic_gi_tx" => traffic::gi_tx(RCB),
            "traffic_gi_rx" => traffic::report_rx(RCB, DS, ffi::IEC61850_REASON_GI, 12, Some(1), None),
            "traffic_poll" => traffic::poll_rx("simpleIOGenericIO/GGIO1[MX]", 4),
            "control_tx" => traffic::control_tx("SELECT", POS, Some(traffic::ctl_val_text(CtlVal::Bool(true)).as_str()), "operator1"),
            "control_rx_positive" => traffic::control_rx("SELECT", POS, None),
            "control_rx_negative" => traffic::control_rx("OPERATE", POS, Some(10)),
            "control_termination" => traffic::termination_rx(POS, true),
            "traffic_dropped" => {
                // batas 1/detik, 13 baris di detik 1 -> 12 dilewati, ringkasan di detik 2 (tanpa timestamp)
                let t = TrafficLog::new(true, 1);
                for _ in 0..13 {
                    t.data_lines(1, || "x".into());
                }
                assert_eq!(t.data_lines(2, || "x".into())[0], line, "{name}");
                continue;
            }
            _ => {
                assert!(["[INFO] ", "[WARN] ", "[ERROR] "].iter().any(|p| line.starts_with(p)), "{name}: {line}");
                continue;
            }
        };
        let (ts, body) = line.split_once(' ').unwrap_or_else(|| panic!("{name}: {line}"));
        assert_eq!(body, expected, "{name}");
        assert!(ts.ends_with('Z') && ts.len() == traffic::ts().len(), "{name}: timestamp {ts}");
    }
}

#[test]
fn contoh_manual_set_response() {
    for (name, v) in examples_of("produce", TOPIC_MANUAL_SET_RESPONSE) {
        let status = str_of(&v, "status");
        let m = ManualSetEventResponse {
            status: status.into(),
            protocol: str_of(&v, "protocol").into(),
            tag: str_of(&v, "tag").into(),
            manual_set: v.get("manual_set").and_then(Value::as_bool),
            user: v.get("user").and_then(Value::as_str).map(String::from),
            error: v.get("error").and_then(Value::as_str).map(String::from),
            timestamp: int_of(&v, "timestamp"),
        };
        assert_eq!(serde_json::to_value(&m).unwrap(), v, "{name}");
        assert!(matches!(status, "SUCCESS" | "FAILED"), "{name}");
        assert_eq!((m.manual_set.is_some(), m.error.is_some()), (status == "SUCCESS", status == "FAILED"), "{name}");
    }
}
