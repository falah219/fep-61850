//! Event COMMAND (RTU_EVENTS) -> select / operate / cancel IEC 61850 -> respons RTU_COMMAND.
//! Format payload & respons sama dengan `kafka/handlers/rtu_event/command` FEP IEC 104.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::iec61850::acquisition::Ctl;
use crate::kafka::models::{RtuEventMessage, PROTOCOL};
use crate::manager::Manager;

const TOPIC_RTU_COMMAND: &str = "RTU_COMMAND";
/// Batas tunggu hasil command dari thread IED (termasuk CommandTermination).
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CommandAction {
    Select,
    Operate,
    Cancel,
}

impl CommandAction {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SELECT_RTU" => Some(Self::Select),
            "OPERATE_RTU" => Some(Self::Operate),
            "CANCEL_RTU" => Some(Self::Cancel),
            _ => None,
        }
    }

    /// Nilai field `event` di respons (sama dengan IEC 104).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Operate => "OPERATE",
            Self::Cancel => "CANCEL",
        }
    }
}

/// Perintah yang dijalankan di thread IED.
#[derive(Debug)]
pub struct CommandReq {
    pub action: CommandAction,
    pub ioa: i32,
    pub value: Option<i32>,
    pub or_ident: String,
}

pub struct CommandJob {
    pub req: CommandReq,
    pub reply: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CtlVal {
    Bool(bool),
    Int(i32),
}

/// value payload -> ctlVal sesuai CDC titik kontrol.
pub fn ctl_value(cdc: &str, value: i32) -> Result<CtlVal, String> {
    match cdc {
        "SPC" => Ok(CtlVal::Bool(value != 0)),
        // ponytail: pemetaan standar DCS IEC 104 (2=ON/CLOSE, 1=OFF/OPEN). BELUM dikonfirmasi tim,
        // dokumen COMMAND_TEST_PAYLOADS IEC 104 menulis sebaliknya. Ubah di sini kalau perlu.
        "DPC" => match value {
            2 => Ok(CtlVal::Bool(true)),
            1 => Ok(CtlVal::Bool(false)),
            v => Err(format!("value DPC harus 1 (OPEN) atau 2 (CLOSE), bukan {v}")),
        },
        "INC" => Ok(CtlVal::Int(value)),
        other => Err(format!("CDC {other} belum didukung untuk command")),
    }
}

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
    port: u16,
    value: Option<Value>,
    #[serde(rename = "type")]
    command_type: Option<String>,
    id_rtu: Option<i32>,
    key: Option<String>,
    ioa: Option<Value>,
    user: Option<String>,
}

fn as_i32(v: Option<&Value>) -> Option<i32> {
    match v? {
        Value::Number(n) => n.as_i64().map(|v| v as i32),
        Value::String(s) => s.trim().parse().ok(),
        Value::Bool(b) => Some(*b as i32),
        _ => None,
    }
}

/// Utamakan objek `data`; kalau tidak ada, pakai field datar (sama dengan IEC 104).
fn parse_command_data(m: &RtuEventMessage) -> CommandData {
    if let Some(Ok(mut cd)) = m.data.as_ref().filter(|d| d.is_object()).map(CommandData::deserialize) {
        if cd.ip_address.is_empty() {
            cd.ip_address = m.ip_address.clone();
        }
        if cd.id_rtu.is_none() {
            cd.id_rtu = m.id_rtu;
        }
        if cd.user.is_none() {
            cd.user = m.user.clone();
        }
        return cd;
    }
    CommandData {
        action: m.action.clone().unwrap_or_default(),
        tag: m.tag.clone(),
        ip_address: m.ip_address.clone(),
        port: m.port.unwrap_or(102) as u16,
        value: m.value.clone(),
        command_type: m.command_type.clone(),
        id_rtu: m.id_rtu,
        key: m.key.clone(),
        ioa: m.ioa.clone().map(Value::String),
        user: m.user.clone(),
    }
}

/// Bagian SINKRON: parse lalu masukkan command ke antrean thread IED saat itu juga, jadi urutan
/// command ke satu IED = urutan kedatangan Kafka (SELECT selalu sebelum OPERATE).
/// Future yang dikembalikan (tunggu hasil + kirim respons RTU_COMMAND) boleh di-`tokio::spawn`.
pub fn submit(mgr: &Arc<Manager>, m: RtuEventMessage) -> Option<impl Future<Output = ()> + Send + 'static> {
    let cd = parse_command_data(&m);
    let Some(action) = CommandAction::parse(&cd.action) else {
        warn!("[COMMAND] action tidak dikenal: {:?}", cd.action);
        return None;
    };
    let value = as_i32(cd.value.as_ref());
    let ioa = as_i32(cd.ioa.as_ref()).or_else(|| cd.tag.as_deref().and_then(ioa_from_tag));
    info!("[COMMAND] {} ip={} id_rtu={:?} ioa={ioa:?} value={value:?} key={:?}", action.as_str(), cd.ip_address, cd.id_rtu, cd.key);

    let queued = enqueue(mgr, &cd, action, ioa, value);
    let mgr = Arc::clone(mgr);
    Some(async move {
        let result = match queued {
            Ok(rx) => match tokio::time::timeout(COMMAND_TIMEOUT, rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("thread IED berhenti sebelum command selesai".into()),
                Err(_) => Err("timeout menunggu hasil command".into()),
            },
            Err(e) => Err(e),
        };
        respond(&mgr, &cd, action, ioa, value, &result);
    })
}

fn respond(mgr: &Manager, cd: &CommandData, action: CommandAction, ioa: Option<i32>, value: Option<i32>, result: &Result<(), String>) {
    match result {
        Ok(()) => info!("[COMMAND] {} sukses ioa={ioa:?}", action.as_str()),
        Err(e) => error!("[COMMAND] {} gagal ioa={ioa:?}: {e}", action.as_str()),
    }
    let (status, msg) = match result {
        Ok(()) => ("active", format!("{} success RTU status is Active", action.as_str())),
        Err(e) => ("inactive", format!("{} failed: {e} - RTU status is Inactive", action.as_str())),
    };
    let key = cd.key.clone().unwrap_or_default();
    let response = json!({
        "hostname": cd.ip_address,
        "port": cd.port,
        "protocol": PROTOCOL,
        "tag": cd.tag.clone().unwrap_or_default(),
        "command_type": cd.command_type.clone().unwrap_or_default(),
        "command_value": value,
        "qualifier": 0,
        "event": action.as_str(),
        "status": status,
        "msg": msg,
        "key": key,
        "time": chrono::Utc::now().timestamp_millis(),
    });
    let topic_key = if key.is_empty() { "unknown" } else { key.as_str() };
    mgr.publisher.producer.send_raw(TOPIC_RTU_COMMAND, topic_key, &response);
}

/// Validasi + kirim ke antrean thread IED (tanpa menunggu).
fn enqueue(
    mgr: &Manager,
    cd: &CommandData,
    action: CommandAction,
    ioa: Option<i32>,
    value: Option<i32>,
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
    fn action_dari_teks() {
        assert_eq!(CommandAction::parse("select_rtu"), Some(CommandAction::Select));
        assert_eq!(CommandAction::parse("OPERATE_RTU"), Some(CommandAction::Operate));
        assert_eq!(CommandAction::parse("CANCEL_RTU"), Some(CommandAction::Cancel));
        assert_eq!(CommandAction::parse("SELECT"), None);
        assert_eq!(CommandAction::Operate.as_str(), "OPERATE");
    }

    #[test]
    fn nilai_ctl() {
        assert_eq!(ctl_value("SPC", 1), Ok(CtlVal::Bool(true)));
        assert_eq!(ctl_value("SPC", 0), Ok(CtlVal::Bool(false)));
        assert_eq!(ctl_value("DPC", 2), Ok(CtlVal::Bool(true)));
        assert_eq!(ctl_value("DPC", 1), Ok(CtlVal::Bool(false)));
        assert!(ctl_value("DPC", 0).is_err());
        assert_eq!(ctl_value("INC", 5), Ok(CtlVal::Int(5)));
        assert!(ctl_value("APC", 1).is_err());
    }

    #[test]
    fn ioa_dari_tag() {
        assert_eq!(ioa_from_tag("TAG_NAME_6000"), Some(6000));
        assert_eq!(ioa_from_tag("SIM1_SPCSO1_CO"), None);
        assert_eq!(ioa_from_tag("NO_IOA"), None);
        assert_eq!(ioa_from_tag("X_"), None);
    }

    #[test]
    fn parse_payload_nested_dan_datar() {
        let nested: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-sim-1",
                "data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"type":"SPC","id_rtu":9001,"key":"k1","user":"op1"}}"#,
        )
        .unwrap();
        let cd = parse_command_data(&nested);
        assert_eq!(cd.action, "OPERATE_RTU");
        assert_eq!(cd.ip_address, "ied-sim-1");
        assert_eq!(as_i32(cd.ioa.as_ref()), Some(3001));
        assert_eq!(as_i32(cd.value.as_ref()), Some(1));
        assert_eq!(cd.user.as_deref(), Some("op1"));

        let flat: RtuEventMessage = serde_json::from_str(
            r#"{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-sim-1",
                "action":"SELECT_RTU","ioa":"3001","value":"1","id_rtu":9001,"key":"k2"}"#,
        )
        .unwrap();
        let cd = parse_command_data(&flat);
        assert_eq!(cd.action, "SELECT_RTU");
        assert_eq!(as_i32(cd.ioa.as_ref()), Some(3001));
        assert_eq!(as_i32(cd.value.as_ref()), Some(1));
    }
}
