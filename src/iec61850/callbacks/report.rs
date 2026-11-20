//! Callback report libIEC61850 (padanan `iec104/callbacks/asdu.rs` FEP IEC 104).
//! Dipanggil dari thread penerima libIEC61850, bukan thread IED.

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, PoisonError};

use tracing::warn;

use crate::domain::{Point, PointState};
use crate::ffi;
use crate::iec61850::acquisition::{decode, decode_unmapped, MemberPlan, RtuCtx};
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
    /// Titik virtual anggota dataset tanpa titik (RTU_DATAS_UNMAPPED, `iec61850::unmapped`) + nilai terakhirnya
    pub(crate) unmapped: Vec<Point>,
    pub(crate) unmapped_state: Mutex<Vec<PointState>>,
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

/// BRCB dari tag `.BR.` di referensi (LN apa saja), bukan dari nama RCB.
fn is_brcb(rcb_ref: &str) -> bool {
    rcb_ref.contains(".BR.")
}

/// Hasil decode satu report: (indeks titik, action) termapping & titik virtual, gabungan reason, jumlah entri.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Collected {
    pub(crate) mapped: Vec<(usize, &'static str)>,
    pub(crate) unmapped: Vec<(usize, &'static str)>,
    pub(crate) mask: u32,
    pub(crate) entries: usize,
}

/// Beberapa anggota untuk satu titik (stVal + q + t): satu entri, action tertinggi.
fn merge(actions: &mut Vec<(usize, &'static str)>, touched: &[usize], action: &'static str) {
    for &p in touched {
        match actions.iter_mut().find(|(q, _)| *q == p) {
            Some(slot) if action_rank(action) > action_rank(slot.1) => slot.1 = action,
            Some(_) => {}
            None => actions.push((p, action)),
        }
    }
}

/// Decode semua anggota yang termuat di report ke `state` (titik termapping) dan `ustate` (titik virtual).
/// `element(i)` = (reason-for-inclusion, nilai anggota ke-i; null kalau tidak termuat).
///
/// # Safety
/// Nilai dari `element` harus null atau MmsValue valid sesuai tipe anggota ke-i selama pemanggilan.
pub(crate) unsafe fn collect(
    ctx: &ReportCtx,
    state: &mut [PointState],
    ustate: &mut [PointState],
    element: impl Fn(usize) -> (u32, *mut ffi::MmsValue),
) -> Collected {
    let mut c = Collected::default();
    let mut touched = Vec::new();
    for (i, member) in ctx.members.iter().enumerate() {
        let (reason, v) = element(i);
        if reason == ffi::IEC61850_REASON_NOT_INCLUDED {
            continue;
        }
        c.mask |= reason;
        c.entries += 1;
        let action = reason_action(reason).unwrap_or("DATA_UPDATE");
        if v.is_null() {
            continue;
        }
        touched.clear();
        // SAFETY: v valid (syarat pemanggil)
        unsafe { decode(member, v, &ctx.rtu.points, state, &mut touched) };
        merge(&mut c.mapped, &touched, action);
        touched.clear();
        // SAFETY: idem; anggota tanpa titik virtual -> tidak ada pick
        unsafe { decode_unmapped(member, v, &ctx.unmapped, ustate, &mut touched) };
        merge(&mut c.unmapped, &touched, action);
    }
    c
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
        // hanya callback RCB ini yang memakai ustate
        let mut ustate = ctx.unmapped_state.lock().unwrap_or_else(PoisonError::into_inner);
        let element = |i: usize| {
            // SAFETY: report & values valid selama callback
            let reason = unsafe { ffi::ClientReport_getReasonForInclusion(report, i as i32) } as u32;
            let v = if reason == ffi::IEC61850_REASON_NOT_INCLUDED {
                std::ptr::null_mut()
            } else {
                // SAFETY: idem
                unsafe { ffi::MmsValue_getElement(values, i as i32) }
            };
            (reason, v)
        };
        // SAFETY: element mengembalikan elemen dataset report ini (tipe = anggota ke-i)
        let c = unsafe { collect(ctx, &mut state, &mut ustate, element) };
        // Waktu entri report: dipakai untuk titik yang tidak punya atribut `t`.
        // SAFETY: report valid selama callback
        let entry_time = unsafe { ffi::ClientReport_hasTimestamp(report) }
            .then(|| unsafe { ffi::ClientReport_getTimestamp(report) } as i64)
            .filter(|&ms| ms > 0);
        // titik yang ada di dua RCB tetap ber-rcb pengirim report. stVal + q + t satu titik = satu pesan.
        let origin = ctx.origin();
        for &(p, action) in &c.mapped {
            rtu.publish(p, &state[p], action, entry_time, origin);
        }
        for &(p, action) in &c.unmapped {
            rtu.publish_unmapped(&ctx.unmapped[p], &ustate[p], action, entry_time, origin);
        }
        drop(ustate);
        drop(state);
        // EntryID dicatat SETELAH report diproses: saat reconnect IED melanjutkan dari entri berikutnya.
        // SAFETY: nilai milik report, langsung disalin
        let entry_id = unsafe { octet_bytes(ffi::ClientReport_getEntryId(report)) };
        // SAFETY: report valid selama callback
        let seq = unsafe { ffi::ClientReport_hasSeqNum(report) }.then(|| unsafe { ffi::ClientReport_getSeqNum(report) });
        let brcb = is_brcb(&ctx.rcb_ref);
        rtu.log_data(|| {
            let id = entry_id.as_deref().filter(|_| brcb);
            traffic::report_rx(&ctx.rcb_ref, &ctx.dataset, c.mask, c.entries, seq, id)
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
            .map(|(r, _)| ReportCtx {
                rtu: Arc::clone(&rtu),
                rcb_ref: r.clone(),
                dataset: "LD/LLN0.ds".into(),
                members: vec![],
                unmapped: vec![],
                unmapped_state: Mutex::default(),
                alive: Arc::default(),
            })
            .collect()
    }

    /// (a) BRCB, (b) URCB, (e) format = rcb_reference DB; dua RCB dataset sama -> rcb masing-masing.
    #[tokio::test]
    async fn rcb_dari_rcb_penerima_format_db() {
        // + RCB di LN selain LLN0 (IED 9302): rcb = rcb_reference apa adanya, tanpa asumsi LLN0
        let db = [
            "iec61850SC01_LD0/LLN0.BR.brcbST01",
            "iec61850SC01_LD0/LLN0.BR.brcbST02",
            "iec61850SC01_LD0/LLN0.RP.urcbMX01",
            "iec61850SC01_LD0/MainCSWI1.RP.brcbName01",
        ];
        let ctxs = report_ctxs(&db);
        for (c, want) in ctxs.iter().zip(db) {
            let o = c.origin();
            // persis rcb_reference DB (bukan format MMS LLN0$BR$...)
            assert_eq!((o.source(), o.rcb()), ("REPORT", Some(want)));
        }
        assert!(ctxs[2].origin().rcb().is_some_and(|r| r.contains(".RP.")));
        // nama "brcb..." di .RP. tetap URCB (EntryID hanya untuk .BR.)
        assert!(!is_brcb(&ctxs[3].rcb_ref) && is_brcb(&ctxs[0].rcb_ref));
    }
}
