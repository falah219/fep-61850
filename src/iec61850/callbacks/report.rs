//! Callback report libIEC61850 (padanan `iec104/callbacks/asdu.rs` FEP IEC 104).
//! Dipanggil dari thread penerima libIEC61850, bukan thread IED.

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use tracing::warn;

use crate::ffi;
use crate::iec61850::acquisition::{decode, MemberPlan, RtuCtx};
use crate::iec61850::client::{octet_bytes, Liveness};
use crate::iec61850::mapper::{action_rank, reason_action};

pub(crate) struct ReportCtx {
    pub(crate) rtu: Arc<RtuCtx>,
    pub(crate) rcb_ref: String,
    pub(crate) members: Vec<MemberPlan>,
    /// Report masuk = tanda hidup koneksi
    pub(crate) alive: Arc<Liveness>,
}

pub(crate) unsafe extern "C" fn on_report(param: *mut c_void, report: ffi::ClientReport) {
    // Panic tidak boleh menyeberang ke C.
    let res = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: param = &ReportCtx yang hidup lebih lama dari koneksi (lihat session()).
        let ctx = unsafe { &*(param as *const ReportCtx) };
        ctx.alive.touch();
        let rtu = &ctx.rtu;
        // SAFETY: report & nilainya valid selama callback; tidak disimpan
        let values = unsafe { ffi::ClientReport_getDataSetValues(report) };
        if values.is_null() {
            return;
        }
        let Ok(mut state) = rtu.state.lock() else { return };
        let mut actions: Vec<(usize, &'static str)> = Vec::new();
        for (i, member) in ctx.members.iter().enumerate() {
            let reason = unsafe { ffi::ClientReport_getReasonForInclusion(report, i as i32) } as u32;
            if reason == ffi::IEC61850_REASON_NOT_INCLUDED {
                continue;
            }
            let action = reason_action(reason).unwrap_or("DATA_UPDATE");
            let v = unsafe { ffi::MmsValue_getElement(values, i as i32) };
            if v.is_null() {
                continue;
            }
            let mut touched = Vec::new();
            unsafe { decode(member, v, &rtu.points, &mut state, &mut touched) };
            for p in touched {
                match actions.iter_mut().find(|(q, _)| *q == p) {
                    Some(slot) if action_rank(action) > action_rank(slot.1) => slot.1 = action,
                    Some(_) => {}
                    None => actions.push((p, action)),
                }
            }
        }
        // Waktu entri report: dipakai untuk titik yang tidak punya atribut `t`.
        // SAFETY: report valid selama callback
        let entry_time = unsafe { ffi::ClientReport_hasTimestamp(report) }
            .then(|| unsafe { ffi::ClientReport_getTimestamp(report) } as i64)
            .filter(|&ms| ms > 0);
        for (p, action) in actions {
            rtu.publish(p, &state[p], action, entry_time);
        }
        drop(state);
        // EntryID dicatat SETELAH report diproses: saat reconnect IED melanjutkan dari entri berikutnya.
        // SAFETY: nilai milik report, langsung disalin
        if let Some(id) = unsafe { octet_bytes(ffi::ClientReport_getEntryId(report)) } {
            rtu.set_entry_id(&ctx.rcb_ref, id);
        }
    }));
    if res.is_err() {
        warn!("panic di report handler (diabaikan)");
    }
}
