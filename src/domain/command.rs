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
}
