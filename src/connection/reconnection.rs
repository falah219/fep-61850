//! Loop reconnect satu IED (padanan `connection/reconnection.rs` FEP IEC 104).

use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::iec61850::acquisition::{session, Ctl, End, RtuCtx};

/// Loop: konek, akuisisi sampai putus, tunggu `reconnect_time_ms`, ulangi. Berhenti saat Ctl::Stop.
/// `reconnect_retry_count` = 0 berarti tanpa batas.
pub fn run_ied(ctx: Arc<RtuCtx>, rx: Receiver<Ctl>) {
    let r = &ctx.rtu;
    let producer = &ctx.publisher.producer;
    let mut failures: u32 = 0;
    loop {
        let result = session(&ctx, &rx);
        let was_connected = ctx.connected.swap(false, Ordering::SeqCst);
        if was_connected {
            producer.send_status(&r.ip_address, r.id_rtu, 0);
        }
        match result {
            Ok(End::Stopped) => {
                info!(id_rtu = r.id_rtu, "thread IED dihentikan");
                return;
            }
            Ok(End::Disconnected) => {
                failures = 0;
                warn!(id_rtu = r.id_rtu, "koneksi ke IED terputus");
                producer.send_log(&r.ip_address, r.id_rtu, "WARN", "koneksi MMS terputus");
            }
            Err(e) => {
                failures += 1;
                warn!(id_rtu = r.id_rtu, attempt = failures, "sesi IED gagal: {e}");
                // Status 0 + log hanya di kegagalan pertama supaya tidak banjir (heartbeat tetap jalan).
                if failures == 1 {
                    if !was_connected {
                        producer.send_status(&r.ip_address, r.id_rtu, 0);
                    }
                    producer.send_log(&r.ip_address, r.id_rtu, "ERROR", &format!("gagal konek MMS: {e}"));
                }
                let max = r.reconnect_retry_count;
                if max > 0 && failures >= max {
                    warn!(id_rtu = r.id_rtu, "berhenti reconnect setelah {failures} percobaan");
                    producer.send_log(&r.ip_address, r.id_rtu, "ERROR", "berhenti reconnect, batas percobaan tercapai");
                    return;
                }
            }
        }
        match rx.recv_timeout(Duration::from_millis(r.reconnect_time_ms)) {
            Ok(Ctl::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Ok(Ctl::Command(job)) => {
                let _ = job.reply.send(Err("IED tidak terkoneksi".into()));
            }
            Ok(Ctl::Browse(reply)) => {
                let _ = reply.send(Err("IED tidak terkoneksi".into()));
            }
            Ok(Ctl::Gi | Ctl::Wake) | Err(RecvTimeoutError::Timeout) => {}
        }
    }
}
