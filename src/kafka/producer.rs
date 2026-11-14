//! Producer Kafka (padanan `kafka/producer.rs` FEP IEC 104).

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use super::models::{DataMessage, RtuLoggingMessage, RtuStatusMessage};
use super::{TOPIC_RTU_LOGGING, TOPIC_RTU_STATUS};
use crate::config::KafkaConfig;

pub struct KafkaProducer {
    producer: Arc<BaseProducer>,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig) -> Result<Self, String> {
        let mut cc = ClientConfig::new();
        for (k, v) in config.to_producer_config() {
            cc.set(k, v);
        }
        let producer: BaseProducer = cc.create().map_err(|e| format!("gagal membuat producer: {e}"))?;
        let producer = Arc::new(producer);

        // Thread poll di belakang, sama seperti FEP IEC 104 (delivery report & callback)
        let p = Arc::clone(&producer);
        std::thread::spawn(move || loop {
            p.poll(Duration::from_millis(100));
        });

        info!("Kafka producer dibuat");
        Ok(Self { producer })
    }

    pub fn send_with_key(&self, topic: &str, key: &str, payload: &str) {
        let record: BaseRecord<str, str> = BaseRecord::to(topic).key(key).payload(payload);
        if let Err((e, _)) = self.producer.send(record) {
            warn!("[KAFKA] Dropped message to {topic}: {e}");
        }
    }

    pub fn send_data(&self, topic: &str, key: &str, message: &DataMessage) {
        match serde_json::to_string(message) {
            Ok(json) => self.send_with_key(topic, key, &json),
            Err(e) => warn!("[KAFKA] Gagal serialize DataMessage: {e}"),
        }
    }

    fn send_json<T: Serialize>(&self, topic: &str, key: &str, msg: &T) {
        match serde_json::to_string(msg) {
            Ok(json) => self.send_with_key(topic, key, &json),
            Err(e) => warn!("[KAFKA] Gagal serialize pesan {topic}: {e}"),
        }
    }

    /// RTU_STATUS: 1 = terkoneksi, 0 = putus. Key = id_rtu (sama dengan IEC 104).
    pub fn send_status(&self, ip: &str, id_rtu: i32, status: i32) {
        self.send_json(TOPIC_RTU_STATUS, &id_rtu.to_string(), &RtuStatusMessage::new(ip, id_rtu, status, None));
    }

    /// RTU_STATUS dengan channel (hasil ping jaringan).
    pub fn send_status_with_channel(&self, ip: &str, id_rtu: i32, channel: i32, status: i32) {
        let msg = RtuStatusMessage::new(ip, id_rtu, status, Some(channel));
        self.send_json(TOPIC_RTU_STATUS, &id_rtu.to_string(), &msg);
    }

    /// Status sistem FEP: 1 = start, 0 = stop (`ip_address: "system"`, `id_rtu: 0`).
    pub fn send_system_status(&self, status: i32) {
        self.send_status("system", 0, status);
    }

    /// RTU_LOGGING, format `[LEVEL] pesan` (sama dengan `send_formatted_log` IEC 104).
    pub fn send_log(&self, ip: &str, id_rtu: i32, level: &str, message: &str) {
        let msg = RtuLoggingMessage::new(ip, id_rtu, &format!("[{level}] {message}"));
        self.send_json(TOPIC_RTU_LOGGING, &id_rtu.to_string(), &msg);
    }

    pub fn send_raw(&self, topic: &str, key: &str, msg: &impl Serialize) {
        self.send_json(topic, key, msg);
    }

    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            warn!("[KAFKA] Flush failed: {e}");
        }
    }
}
