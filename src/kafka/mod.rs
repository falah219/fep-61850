//! Producer Kafka + format pesan. Format JSON identik dengan FEP IEC 104 (`kafka/models.rs`).

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::config::KafkaConfig;

pub const TOPIC_RTU_DATAS: &str = "RTU_DATAS";
pub const TOPIC_DATAPOINTS: &str = "DATAPOINTS";

/// Salinan `DataMessage` FEP IEC 104. Urutan & nama field JANGAN diubah.
#[derive(Serialize)]
pub struct DataMessage<'a> {
    pub ip_address: &'a str,
    pub protocol: &'static str,
    pub id_rtu: i32,
    pub action: &'a str,
    pub tag: &'a str,
    pub value: &'a serde_json::Value,
    pub address: i32,
    pub ca: i32,
    pub asdu: i32,
    #[serde(rename = "type")]
    pub typ: &'a str,
    pub quality: &'static str,
    pub flag: &'a str,
    pub group: &'a str,
    pub general_action: Option<i32>,
    pub time: i64,
    pub time_desc: &'static str,
}

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

    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            warn!("[KAFKA] Flush failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Bentuk JSON harus sama persis dengan test `data_message_keeps_json_shape` di FEP IEC 104.
    #[test]
    fn bentuk_json_sama_dengan_iec104() {
        let value = json!(2);
        let m = DataMessage {
            ip_address: "10.0.0.1",
            protocol: "IEC61850",
            id_rtu: 7,
            action: "DATA_CHANGE",
            tag: "TAG",
            value: &value,
            address: 1001,
            ca: 0,
            asdu: 0,
            typ: "DPS",
            quality: "GOOD",
            flag: "GOOD",
            group: "",
            general_action: Some(3),
            time: 1,
            time_desc: "RTU",
        };
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            r#"{"ip_address":"10.0.0.1","protocol":"IEC61850","id_rtu":7,"action":"DATA_CHANGE","tag":"TAG","value":2,"address":1001,"ca":0,"asdu":0,"type":"DPS","quality":"GOOD","flag":"GOOD","group":"","general_action":3,"time":1,"time_desc":"RTU"}"#
        );
    }
}
