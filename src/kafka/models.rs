//! Format pesan Kafka. Disalin dari `kafka/models.rs` FEP IEC 104 (nama & bentuk field sama),
//! hanya `protocol` diisi "IEC61850".

use serde::{Deserialize, Deserializer, Serialize};

pub const PROTOCOL: &str = "IEC61850";

/// IOA boleh string atau angka (sama seperti IEC 104).
fn deserialize_ioa<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    use serde::de::Error;
    use serde_json::Value;
    match Option::<Value>::deserialize(d)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(Value::Number(n)) => Ok(Some(n.to_string())),
        Some(_) => Err(Error::custom("IOA must be a string or number")),
    }
}

/// Hanya karakter aman untuk IP (sama dengan sanitasi IEC 104).
pub fn safe_ip(ip: &str) -> String {
    ip.chars()
        .filter(|c| c.is_ascii() && (c.is_alphanumeric() || *c == '.' || *c == ':'))
        .collect()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Debug, Clone, Serialize)]
pub struct RtuStatusMessage {
    pub ip_address: String,
    pub id_rtu: i32,
    pub protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<i32>,
    pub status: i32,
    pub time: i64,
}

impl RtuStatusMessage {
    pub fn new(ip: &str, id_rtu: i32, status: i32, channel: Option<i32>) -> Self {
        Self { ip_address: safe_ip(ip), id_rtu, protocol: PROTOCOL.into(), channel, status, time: now_ms() }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RtuLoggingMessage {
    pub ip_address: String,
    pub protocol: String,
    pub id_rtu: i32,
    pub logging: String,
    pub time: i64,
}

impl RtuLoggingMessage {
    pub fn new(ip: &str, id_rtu: i32, logging: &str) -> Self {
        Self {
            ip_address: safe_ip(ip),
            protocol: PROTOCOL.into(),
            id_rtu,
            logging: logging.chars().filter(char::is_ascii).collect(),
            time: now_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RtuEventMessage {
    pub event: String,
    pub protocol: String,
    #[serde(default)]
    pub ip_address: String,
    pub port: Option<i32>,
    pub state_rtu: Option<bool>,
    pub data: Option<serde_json::Value>,
    pub action: Option<String>,
    pub tag: Option<String>,
    #[serde(rename = "type")]
    pub command_type: Option<String>,
    pub value: Option<serde_json::Value>,
    #[serde(default, deserialize_with = "deserialize_ioa")]
    pub ioa: Option<String>,
    pub key: Option<String>,
    pub id_rtu: Option<i32>,
    pub description: Option<String>,
    pub qualifier: Option<u8>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MasterDatapointEventMessage {
    pub id_rtu: i32,
    pub protocol: String,
    pub ioa: i32,
    pub event: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RtuCommandConfigMessage {
    pub action: String,
    pub protocol: String,
    pub ip_address: String,
    pub ioa: Option<i32>,
    pub value: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CaptureDataChangeMessage {
    pub table: String,
    pub schema: String,
    pub operation: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MasterRtuEventMessage {
    pub id_rtu: i32,
    pub protocol: String,
    pub event: String,
    pub new_data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManualSetEventMessage {
    pub event: String,
    pub protocol: String,
    pub tag: String,
    pub manual_set: bool,
    pub id_rtu: i32,
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManualSetEventResponse {
    pub status: String,
    pub protocol: String,
    pub tag: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manual_set: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_json_sama_dengan_iec104() {
        let mut m = RtuStatusMessage::new("10.0.0.1", 7, 1, None);
        m.time = 5;
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            r#"{"ip_address":"10.0.0.1","id_rtu":7,"protocol":"IEC61850","status":1,"time":5}"#
        );
        let mut m = RtuStatusMessage::new("10.0.0.1", 7, 0, Some(2));
        m.time = 5;
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            r#"{"ip_address":"10.0.0.1","id_rtu":7,"protocol":"IEC61850","channel":2,"status":0,"time":5}"#
        );
    }

    #[test]
    fn rtu_event_ioa_string_atau_angka() {
        let a: RtuEventMessage =
            serde_json::from_str(r#"{"event":"GI","protocol":"IEC61850","ip_address":"1.2.3.4","ioa":3001}"#).unwrap();
        assert_eq!(a.ioa.as_deref(), Some("3001"));
        let b: RtuEventMessage =
            serde_json::from_str(r#"{"event":"GI","protocol":"IEC61850","ip_address":"1.2.3.4","ioa":"3001"}"#).unwrap();
        assert_eq!(b.ioa.as_deref(), Some("3001"));
    }

    #[test]
    fn ip_disanitasi() {
        assert_eq!(safe_ip("10.0.0.1\u{0}<x>"), "10.0.0.1x");
    }
}
