//! Consumer Kafka. Pola sama dengan FEP IEC 104: thread khusus + runtime current-thread,
//! commit manual tiap 2 detik, pesan di-parse lalu dikirim lewat channel ke handler.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::models::*;
use crate::config::KafkaConfig;

pub enum KafkaEvent {
    RtuEvent(Box<RtuEventMessage>),
    MasterDatapoint(MasterDatapointEventMessage),
    MasterDataObject,
    MasterRtu(MasterRtuEventMessage),
    CommandConfig(RtuCommandConfigMessage),
    CaptureDataChange(CaptureDataChangeMessage),
    ManualSet(ManualSetEventMessage),
}

fn parse(topic: &str, payload: &str) -> Result<Option<KafkaEvent>, String> {
    let e = |err: serde_json::Error| format!("{topic} parse error: {err}");
    Ok(Some(match topic {
        "RTU_EVENTS" => KafkaEvent::RtuEvent(Box::new(serde_json::from_str(payload).map_err(e)?)),
        "MASTER_DATAPOINT_EVENT" => KafkaEvent::MasterDatapoint(serde_json::from_str(payload).map_err(e)?),
        "MASTER_DATA_OBJECT_EVENT" => KafkaEvent::MasterDataObject,
        "MASTER_RTU_EVENTS" => KafkaEvent::MasterRtu(serde_json::from_str(payload).map_err(e)?),
        "RTU_COMMAND_CONFIG_REQUEST" => KafkaEvent::CommandConfig(serde_json::from_str(payload).map_err(e)?),
        "CAPTURE_DATA_CHANGE" => KafkaEvent::CaptureDataChange(serde_json::from_str(payload).map_err(e)?),
        "MANUAL_SET_EVENT" => KafkaEvent::ManualSet(serde_json::from_str(payload).map_err(e)?),
        _ => return Ok(None),
    }))
}

/// Buat consumer, subscribe, lalu jalankan di thread sendiri. Event dikirim ke receiver.
pub fn start(config: &KafkaConfig, topics: &[String]) -> Result<mpsc::Receiver<KafkaEvent>, String> {
    let mut cc = ClientConfig::new();
    for (k, v) in config.to_consumer_config() {
        cc.set(k, v);
    }
    let consumer: StreamConsumer = cc.create().map_err(|e| format!("gagal membuat consumer: {e}"))?;
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

async fn run(consumer: StreamConsumer, tx: mpsc::Sender<KafkaEvent>) {
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
        assert!(matches!(parse("MASTER_DATA_OBJECT_EVENT", "{}"), Ok(Some(KafkaEvent::MasterDataObject))));
        assert!(matches!(parse("TOPIK_LAIN", "{}"), Ok(None)));
        assert!(parse("RTU_EVENTS", "bukan json").is_err());
    }
}
