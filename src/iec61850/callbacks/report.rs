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
use crate::iec61850::traffic;
use crate::kafka::publisher::Origin;

pub(crate) struct ReportCtx {
    pub(crate) rtu: Arc<RtuCtx>,
    pub(crate) rcb_ref: String,
    /// Untuk baris REPORT RX (dataset dari DB / RCB)
    pub(crate) dataset: String,
    pub(crate) members: Vec<MemberPlan>,
    /// Report masuk = tanda hidup koneksi
    pub(crate) alive: Arc<Liveness>,
}

impl ReportCtx {
    /// `source`/`rcb` pesan dari report ini: RCB yang MENERIMA report (konteks callback), bukan tebakan dari
    /// dataset. `rcb_ref` = `iec61850_report_controls.rcb_reference` apa adanya (bukan format MMS `LLN0$BR$...`).
    pub(crate) fn origin(&self) -> Origin<'_> {
        Origin::Report { rcb: &self.rcb_ref }
    }
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
        // untuk baris REPORT RX: gabungan reason & jumlah entri yang termuat
        let (mut mask, mut entries) = (0u32, 0usize);
        for (i, member) in ctx.members.iter().enumerate() {
            let reason = unsafe { ffi::ClientReport_getReasonForInclusion(report, i as i32) } as u32;
            if reason == ffi::IEC61850_REASON_NOT_INCLUDED {
                continue;
            }
            mask |= reason;
            entries += 1;
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
        // titik yang ada di dua RCB tetap ber-rcb pengirim report. stVal + q + t satu titik = satu pesan.
        let origin = ctx.origin();
        for (p, action) in actions {
            rtu.publish(p, &state[p], action, entry_time, origin);
        }
        drop(state);
        // EntryID dicatat SETELAH report diproses: saat reconnect IED melanjutkan dari entri berikutnya.
        // SAFETY: nilai milik report, langsung disalin
        let entry_id = unsafe { octet_bytes(ffi::ClientReport_getEntryId(report)) };
        // SAFETY: report valid selama callback
        let seq = unsafe { ffi::ClientReport_hasSeqNum(report) }.then(|| unsafe { ffi::ClientReport_getSeqNum(report) });
        let brcb = ctx.rcb_ref.contains(".BR.");
        rtu.log_data(|| {
            let id = entry_id.as_deref().filter(|_| brcb);
            traffic::report_rx(&ctx.rcb_ref, &ctx.dataset, mask, entries, seq, id)
        });
        if let Some(id) = entry_id {
            rtu.set_entry_id(&ctx.rcb_ref, id);
        }
    }));
    if res.is_err() {
        warn!("panic di report handler (diabaikan)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectGate;
    use crate::iec61850::client::RcbSettings;

    /// ReportCtx seperti dibuat setup_rcb: satu per baris iec61850_report_controls, rcb_ref = rcb_reference DB.
    fn report_ctxs(rcbs: &[&str]) -> Vec<ReportCtx> {
        let mgr = crate::connection::testing::manager();
        let rows = rcbs.iter().map(|r| (r.to_string(), RcbSettings::default())).collect();
        let rtu = Arc::new(RtuCtx::new(crate::connection::testing::rtu(1), rows, vec![], Arc::clone(&mgr.publisher), Arc::new(ConnectGate::new(1))));
        rtu.rcbs
            .iter()
            .map(|(r, _)| ReportCtx { rtu: Arc::clone(&rtu), rcb_ref: r.clone(), dataset: "LD/LLN0.ds".into(), members: vec![], alive: Arc::default() })
            .collect()
    }

    /// (a) BRCB, (b) URCB, (e) format = rcb_reference DB; dua RCB dataset sama -> rcb masing-masing.
    #[tokio::test]
    async fn rcb_dari_rcb_penerima_format_db() {
        let db = ["iec61850SC01_LD0/LLN0.BR.brcbST01", "iec61850SC01_LD0/LLN0.BR.brcbST02", "iec61850SC01_LD0/LLN0.RP.urcbMX01"];
        let ctxs = report_ctxs(&db);
        for (c, want) in ctxs.iter().zip(db) {
            let o = c.origin();
            // persis rcb_reference DB (bukan format MMS LLN0$BR$...)
            assert_eq!((o.source(), o.rcb()), ("REPORT", Some(want)));
        }
        assert!(ctxs[2].origin().rcb().is_some_and(|r| r.contains(".RP.")));
    }
}
