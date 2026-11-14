//! Batas percobaan konek MMS serentak (`MAX_CONCURRENT_CONNECTS`).

use std::sync::{Condvar, Mutex};

/// Batas jumlah percobaan konek serentak (`MAX_CONCURRENT_CONNECTS`).
pub struct ConnectGate {
    free: Mutex<usize>,
    cv: Condvar,
}

pub struct GatePermit<'a>(&'a ConnectGate);

impl ConnectGate {
    pub fn new(max: usize) -> Self {
        Self { free: Mutex::new(max.max(1)), cv: Condvar::new() }
    }

    pub fn acquire(&self) -> GatePermit<'_> {
        let mut free = self.free.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            free = self.cv.wait(free).unwrap_or_else(|e| e.into_inner());
        }
        *free -= 1;
        GatePermit(self)
    }
}

impl Drop for GatePermit<'_> {
    fn drop(&mut self) {
        let mut free = self.0.free.lock().unwrap_or_else(|e| e.into_inner());
        *free += 1;
        self.0.cv.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn gate_membatasi_koneksi_serentak() {
        let gate = Arc::new(ConnectGate::new(2));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (gate, active, peak) = (Arc::clone(&gate), Arc::clone(&active), Arc::clone(&peak));
                std::thread::spawn(move || {
                    let _p = gate.acquire();
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(20));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }
}
