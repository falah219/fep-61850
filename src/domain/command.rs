//! Perintah kontrol (select / operate / cancel) yang dijalankan thread IED.

use tokio::sync::oneshot;

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
    pub value: Option<f64>,
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
    /// APC (AnalogueValue.f); libIEC61850 membungkus ke struktur sendiri
    Float(f32),
    /// BSC Tcmd (BIT STRING 2 bit): 0 = stop, 1 = lower, 2 = higher
    Tcmd(u8),
}

fn whole(cdc: &str, v: f64) -> Result<i32, String> {
    if v.fract() == 0.0 && v >= f64::from(i32::MIN) && v <= f64::from(i32::MAX) {
        Ok(v as i32)
    } else {
        Err(format!("value {cdc} harus bilangan bulat, bukan {v}"))
    }
}

/// Env `DPC_CLOSE_VALUE`: value payload untuk CLOSE (1 atau 2, default 2). OPEN = nilai lainnya.
pub fn dpc_close_value(env: Option<&str>) -> i32 {
    match env.map(str::trim) {
        Some("1") => 1,
        _ => 2,
    }
}

/// value payload -> ctlVal sesuai CDC titik kontrol. `dpc_close` = env DPC_CLOSE_VALUE (1 atau 2).
pub fn ctl_value(cdc: &str, value: f64, dpc_close: i32) -> Result<CtlVal, String> {
    match cdc {
        "SPC" => Ok(CtlVal::Bool(value != 0.0)),
        // Default 2=CLOSE, 1=OPEN (standar DCS IEC 104). BELUM dikonfirmasi tim: dokumen
        // COMMAND_TEST_PAYLOADS IEC 104 menulis sebaliknya -> bisa dibalik lewat DPC_CLOSE_VALUE=1.
        "DPC" => {
            let close = if dpc_close == 1 { 1 } else { 2 };
            match whole(cdc, value)? {
                v if v == close => Ok(CtlVal::Bool(true)),
                v if v == 3 - close => Ok(CtlVal::Bool(false)),
                v => Err(format!("value DPC harus {} (OPEN) atau {close} (CLOSE), bukan {v}", 3 - close)),
            }
        }
        "INC" | "ENC" => whole(cdc, value).map(CtlVal::Int),
        "BSC" => match whole(cdc, value)? {
            v @ 0..=2 => Ok(CtlVal::Tcmd(v as u8)),
            v => Err(format!("value BSC harus 0 (stop), 1 (lower), atau 2 (higher), bukan {v}")),
        },
        "APC" if value.is_finite() => Ok(CtlVal::Float(value as f32)),
        "APC" => Err(format!("value APC tidak valid: {value}")),
        other => Err(format!("CDC {other} belum didukung untuk command")),
    }
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
        assert_eq!(ctl_value("SPC", 1.0, 2), Ok(CtlVal::Bool(true)));
        assert_eq!(ctl_value("SPC", 0.0, 2), Ok(CtlVal::Bool(false)));
        assert_eq!(ctl_value("DPC", 2.0, 2), Ok(CtlVal::Bool(true)));
        assert_eq!(ctl_value("DPC", 1.0, 2), Ok(CtlVal::Bool(false)));
        assert!(ctl_value("DPC", 0.0, 2).is_err());
        assert!(ctl_value("DPC", 1.5, 2).is_err());
        assert_eq!(ctl_value("INC", 5.0, 2), Ok(CtlVal::Int(5)));
        assert!(ctl_value("XYZ", 1.0, 2).is_err());
    }

    #[test]
    fn dpc_close_bisa_diatur() {
        assert_eq!(dpc_close_value(None), 2);
        assert_eq!(dpc_close_value(Some("2")), 2);
        assert_eq!(dpc_close_value(Some(" 1 ")), 1);
        assert_eq!(dpc_close_value(Some("x")), 2);
        // DPC_CLOSE_VALUE=2 (default): 2 = CLOSE, 1 = OPEN
        assert_eq!(ctl_value("DPC", 2.0, 2), Ok(CtlVal::Bool(true)));
        assert_eq!(ctl_value("DPC", 1.0, 2), Ok(CtlVal::Bool(false)));
        // DPC_CLOSE_VALUE=1: 1 = CLOSE, 2 = OPEN
        assert_eq!(ctl_value("DPC", 1.0, 1), Ok(CtlVal::Bool(true)));
        assert_eq!(ctl_value("DPC", 2.0, 1), Ok(CtlVal::Bool(false)));
        assert!(ctl_value("DPC", 0.0, 1).is_err());
    }

    #[test]
    fn nilai_enc() {
        assert_eq!(ctl_value("ENC", 3.0, 2), Ok(CtlVal::Int(3)));
        assert_eq!(ctl_value("ENC", -1.0, 2), Ok(CtlVal::Int(-1)));
        assert!(ctl_value("ENC", 2.5, 2).is_err());
    }

    #[test]
    fn nilai_bsc() {
        assert_eq!(ctl_value("BSC", 0.0, 2), Ok(CtlVal::Tcmd(0))); // stop
        assert_eq!(ctl_value("BSC", 1.0, 2), Ok(CtlVal::Tcmd(1))); // lower
        assert_eq!(ctl_value("BSC", 2.0, 2), Ok(CtlVal::Tcmd(2))); // higher
        assert!(ctl_value("BSC", 3.0, 2).is_err()); // reserved
        assert!(ctl_value("BSC", -1.0, 2).is_err());
    }

    #[test]
    fn nilai_apc() {
        assert_eq!(ctl_value("APC", 12.5, 2), Ok(CtlVal::Float(12.5)));
        assert_eq!(ctl_value("APC", 7.0, 2), Ok(CtlVal::Float(7.0)));
        assert!(ctl_value("APC", f64::NAN, 2).is_err());
    }
}
