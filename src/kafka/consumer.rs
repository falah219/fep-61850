//! Consumer Kafka. Pola sama dengan FEP IEC 104: thread khusus + runtime current-thread,
//! commit manual tiap 2 detik, pesan di-parse lalu dikirim lewat channel ke handler.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::context::KafkaLogContext;
use super::models::*;
use crate::config::KafkaConfig;

pub enum KafkaEvent {
    RtuEvent(Box<RtuEventMessage>),
    MasterDatapoint(MasterDatapointEventMessage),
    /// Isi pesan (None = bukan JSON) -> handlers::master_data_object
    MasterDataObject(Option<serde_json::Value>),
    MasterRtu(MasterRtuEventMessage),
    CommandConfig(RtuCommandConfigMessage),
    CaptureDataChange(CaptureDataChangeMessage),
    ManualSet(ManualSetEventMessage),
    /// COMMAND IEC61850 yang tidak terbaca `RtuEventMessage`. `key` terbaca -> dibalas RTU_COMMAND inactive;
    /// tanpa key -> tidak bisa dicocokkan, hanya log + RTU_LOGGING WARN.
    InvalidCommand { action: String, key: Option<String>, ip_address: String, id_rtu: Option<i32>, error: String },
}

pub(crate) fn parse(topic: &str, payload: &str) -> Result<Option<KafkaEvent>, String> {
    let e = |err: serde_json::Error| format!("{topic} parse error: {err}");
    Ok(Some(match topic {
        "RTU_EVENTS" => match serde_json::from_str(payload) {
            Ok(m) => KafkaEvent::RtuEvent(Box::new(m)),
            Err(err) => invalid_command(payload, &err).ok_or_else(|| e(err))?,
        },
        "MASTER_DATAPOINT_EVENT" => KafkaEvent::MasterDatapoint(serde_json::from_str(payload).map_err(e)?),
        "MASTER_DATA_OBJECT_EVENT" => KafkaEvent::MasterDataObject(serde_json::from_str(payload).ok()),
        "MASTER_RTU_EVENTS" => KafkaEvent::MasterRtu(serde_json::from_str(payload).map_err(e)?),
        "RTU_COMMAND_CONFIG_REQUEST" => KafkaEvent::CommandConfig(serde_json::from_str(payload).map_err(e)?),
        "CAPTURE_DATA_CHANGE" => KafkaEvent::CaptureDataChange(serde_json::from_str(payload).map_err(e)?),
        "MANUAL_SET_EVENT" => KafkaEvent::ManualSet(serde_json::from_str(payload).map_err(e)?),
        _ => return Ok(None),
    }))
}

/// RTU_EVENTS yang gagal di-parse: kalau jelas COMMAND IEC61850, jadikan InvalidCommand
/// (protocol lain TIDAK dijawab: pesan itu milik FEP lain).
fn invalid_command(payload: &str, err: &serde_json::Error) -> Option<KafkaEvent> {
    let json: Option<serde_json::Value> = serde_json::from_str(payload).ok();
    let get = |k: &str| match &json {
        Some(v) => v.get(k).or_else(|| v.get("data").and_then(|d| d.get(k))).and_then(|x| x.as_str()).map(String::from),
        None => scan_str(payload, k),
    };
    if get("event")? != "COMMAND" || !super::event_handler::is_ours(&get("protocol")?) {
        return None;
    }
    Some(KafkaEvent::InvalidCommand {
        action: get("action").unwrap_or_default(),
        key: get("key"),
        ip_address: get("ip_address").unwrap_or_default(),
        id_rtu: json
            .as_ref()
            .and_then(|v| v.get("id_rtu").or_else(|| v.get("data").and_then(|d| d.get("id_rtu"))))
            .and_then(serde_json::Value::as_i64)
            .and_then(|i| i32::try_from(i).ok()),
        error: format!("payload tidak valid: {err}"),
    })
}

/// Nilai string `"field": "..."` pertama di teks JSON yang rusak (tanpa escape).
fn scan_str(raw: &str, field: &str) -> Option<String> {
    let pat = format!("\"{field}\"");
    let rest = &raw[raw.find(&pat)? + pat.len()..];
    let rest = rest.trim_start().strip_prefix(':')?.trim_start().strip_prefix('"')?;
    Some(rest[..rest.find('"')?].to_string())
}

/// Buat consumer, subscribe, lalu jalankan di thread sendiri. Event dikirim ke receiver.
pub fn start(config: &KafkaConfig, topics: &[String]) -> Result<mpsc::Receiver<KafkaEvent>, String> {
    let mut cc = ClientConfig::new();
    for (k, v) in config.to_consumer_config() {
        cc.set(k, v);
    }
    let consumer: StreamConsumer<KafkaLogContext> =
        cc.create_with_context(KafkaLogContext).map_err(|e| format!("gagal membuat consumer: {e}"))?;
    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topic_refs).map_err(|e| format!("gagal subscribe: {e}"))?;
    info!("Kafka consumer group={} subscribe {:?}", config.group_id, topics);

    let (tx, rx) = mpsc::channel(10_000);
    std::thread::Builder::new()
        .name("kafka-consumer".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return error!("gagal membuat runtime consumer: {e}"),
            };
            rt.block_on(run(consumer, tx));
        })
        .map_err(|e| format!("gagal membuat thread consumer: {e}"))?;
    Ok(rx)
}

async fn run(consumer: StreamConsumer<KafkaLogContext>, tx: mpsc::Sender<KafkaEvent>) {
    let mut last_commit = tokio::time::Instant::now();
    loop {
        let message = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                error!("Kafka consumer error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if tx.is_closed() {
            info!("Kafka consumer berhenti");
            return;
        }
        if let Some(Ok(payload)) = message.payload().map(std::str::from_utf8) {
            let topic = message.topic().rsplit('.').next().unwrap_or_default();
            match parse(topic, payload) {
                Ok(Some(event)) => {
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
                Ok(None) => warn!("topik tidak dikenal: {topic}"),
                Err(e) => error!("{e}"),
            }
        }
        if last_commit.elapsed() >= Duration::from_secs(2) {
            if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                warn!("gagal commit offset: {e}");
            }
            last_commit = tokio::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_topik() {
        assert!(matches!(
            parse("RTU_EVENTS", r#"{"event":"GI","protocol":"IEC61850","ip_address":"1.2.3.4"}"#),
            Ok(Some(KafkaEvent::RtuEvent(_)))
        ));
        assert!(matches!(parse("MASTER_DATA_OBJECT_EVENT", "{}"), Ok(Some(KafkaEvent::MasterDataObject(Some(_))))));
        assert!(matches!(parse("MASTER_DATA_OBJECT_EVENT", "bukan json"), Ok(Some(KafkaEvent::MasterDataObject(None)))));
        assert!(matches!(parse("TOPIK_LAIN", "{}"), Ok(None)));
        assert!(parse("RTU_EVENTS", "bukan json").is_err());
    }

    #[test]
    fn command_tidak_valid_tetap_dijawab() {
        let invalid = |p: &str| match parse("RTU_EVENTS", p) {
            Ok(Some(KafkaEvent::InvalidCommand { action, key, error, .. })) => Some((action, key, error)),
            _ => None,
        };
        // id_rtu salah tipe
        let (action, key, error) =
            invalid(r#"{"event":"COMMAND","protocol":"IEC61850","id_rtu":"9001","data":{"action":"OPERATE_RTU","key":"k1"}}"#).unwrap();
        assert_eq!((action.as_str(), key.as_deref()), ("OPERATE_RTU", Some("k1")));
        assert!(error.starts_with("payload tidak valid"));
        // JSON rusak, key masih terbaca
        let (_, key, _) = invalid(r#"{"event":"COMMAND","protocol":"IEC61850","key":"k2","data":{"action":"#).unwrap();
        assert_eq!(key.as_deref(), Some("k2"));
        // tanpa key: tetap InvalidCommand (log + RTU_LOGGING WARN, tanpa RTU_COMMAND)
        let (_, key, _) = invalid(r#"{"event":"COMMAND","protocol":"IEC61850","id_rtu":"x"}"#).unwrap();
        assert_eq!(key, None);
        // protocol lain / bukan COMMAND -> parse error biasa
        assert!(invalid(r#"{"event":"COMMAND","protocol":"IEC104","id_rtu":"x","key":"k"}"#).is_none());
        assert!(invalid(r#"{"event":"GI","protocol":"IEC61850","id_rtu":"x","key":"k"}"#).is_none());
        assert!(parse("RTU_EVENTS", r#"{"event":"COMMAND","protocol":"IEC104","id_rtu":"x"}"#).is_err());
        match parse("RTU_EVENTS", r#"{"event":"COMMAND","protocol":"IEC61850","data":{"id_rtu":9001,"port":"x"},"port":"x"}"#) {
            Ok(Some(KafkaEvent::InvalidCommand { id_rtu, key, .. })) => assert_eq!((id_rtu, key), (Some(9001), None)),
            _ => panic!("harus InvalidCommand"),
        }
    }
}
