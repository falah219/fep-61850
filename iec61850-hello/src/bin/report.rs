//! Fase 5: aktifkan RCB, terima report (tanpa polling), GI setelah konek + GI berkala.
//!
//! Pemakaian: report [host] [port] [rcb_ref ...]
//! Default  : ied-sim-1 102 simpleIOGenericIO/LLN0.BR.EventsBRCB01 simpleIOGenericIO/LLN0.BR.Measurements01
//!
//! Env (opsional). Diisi string kosong = TIDAK ditulis ke IED (pakai setting IED):
//!   TRG_OPS           default "dchg,qchg,integrity,gi"
//!   INTG_PD_MS        default 10000
//!   GI_INTERVAL_SECS  default 300

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use iec61850_hello::{ffi, parse_trg_ops, reason_action, sample, Ied, Rcb};

static START: OnceLock<Instant> = OnceLock::new();

fn ts() -> f64 {
    START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

/// Data yang dibawa ke callback: nama RCB + nama anggota dataset (urutan = urutan nilai di report).
struct Ctx {
    rcb: String,
    members: Vec<String>,
}

/// Dipanggil dari thread penerima libIEC61850 setiap ada report masuk.
unsafe extern "C" fn on_report(param: *mut c_void, report: ffi::ClientReport) {
    // Panic tidak boleh menyeberang ke C: tangkap di sini.
    let res = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: param adalah &Ctx yang hidup lebih lama dari koneksi (lihat run()).
        let ctx = unsafe { &*(param as *const Ctx) };
        // SAFETY: report & nilainya valid selama callback berjalan; tidak disimpan.
        let values = unsafe { ffi::ClientReport_getDataSetValues(report) };
        for (i, name) in ctx.members.iter().enumerate() {
            let reason = unsafe { ffi::ClientReport_getReasonForInclusion(report, i as i32) } as u32;
            if reason == ffi::IEC61850_REASON_NOT_INCLUDED {
                continue;
            }
            let text = if values.is_null() {
                "<tanpa nilai>".to_string()
            } else {
                unsafe { sample(ffi::MmsValue_getElement(values, i as i32)) }.text
            };
            let action = reason_action(reason).unwrap_or("UNKNOWN");
            println!("[{:>9.3}s] {action:<21} {name} = {text}   ({})", ts(), ctx.rcb);
        }
    }));
    if res.is_err() {
        eprintln!("panic di report handler (diabaikan)");
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    ts();
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "ied-sim-1".into());
    let port: i32 = args
        .next()
        .map_or(Ok(102), |p| p.parse().map_err(|_| "port tidak valid".to_string()))?;
    let mut refs: Vec<String> = args.collect();
    if refs.is_empty() {
        refs = vec![
            "simpleIOGenericIO/LLN0.BR.EventsBRCB01".into(),
            "simpleIOGenericIO/LLN0.BR.Measurements01".into(),
        ];
    }

    let trg_ops = parse_trg_ops(&env_or("TRG_OPS", "dchg,qchg,integrity,gi"))?;
    let intg_pd = match env_or("INTG_PD_MS", "10000").trim() {
        "" => None,
        v => Some(v.parse::<u32>().map_err(|_| format!("INTG_PD_MS tidak valid: {v}"))?),
    };
    let gi_every = Duration::from_secs(
        env_or("GI_INTERVAL_SECS", "300")
            .parse()
            .map_err(|_| "GI_INTERVAL_SECS tidak valid".to_string())?,
    );

    // Urutan deklarasi penting: `ctxs` dideklarasikan SEBELUM `ied`, jadi di-drop SESUDAH koneksi
    // ditutup. Callback tidak akan pernah memegang Ctx yang sudah dibebaskan.
    let mut ctxs: Vec<Box<Ctx>> = Vec::new();
    let ied =
        Ied::connect(&host, port).map_err(|e| format!("gagal konek ke {host}:{port}: {e}"))?;
    println!(
        "terkoneksi ke {host}:{port} trg_ops={trg_ops:?} intg_pd_ms={intg_pd:?} gi_interval={}s",
        gi_every.as_secs()
    );

    let mut active = Vec::new();
    for r in refs {
        let rcb = match Rcb::read(&ied, &r) {
            Ok(rcb) => rcb,
            Err(e) => {
                eprintln!("WARN lewati {r}: {e}");
                continue;
            }
        };
        let Some(ds) = rcb.dataset() else {
            eprintln!("WARN lewati {r}: RCB tidak punya dataset");
            continue;
        };
        let (members, _) = ied.dataset_directory(&ds)?;
        let kind = if rcb.is_buffered() { "BRCB" } else { "URCB" };
        println!("  {kind} {r} -> dataset {ds} ({} anggota)", members.len());

        let ctx = Box::new(Ctx { rcb: r.clone(), members });
        let param = &*ctx as *const Ctx as *mut c_void;
        ctxs.push(ctx);
        // SAFETY: Ctx ada di heap (Box) dan hidup sampai setelah `ied` di-drop.
        unsafe { rcb.install_handler(&ied, Some(on_report), param) };

        if let Err(e) = rcb.enable(&ied, trg_ops, intg_pd) {
            eprintln!("WARN gagal enable {r}: {e}");
            continue;
        }
        active.push(rcb);
    }
    if active.is_empty() {
        return Err("tidak ada RCB yang aktif".into());
    }

    // GI langsung setelah konek, lalu berkala.
    let mut last_gi: Option<Instant> = None;
    loop {
        if last_gi.is_none_or(|t| t.elapsed() >= gi_every) {
            for rcb in &active {
                match rcb.gi(&ied) {
                    Ok(()) => println!("[{:>9.3}s] GI dikirim ke {}", ts(), rcb.reference),
                    Err(e) => eprintln!("WARN GI gagal {}: {e}", rcb.reference),
                }
            }
            last_gi = Some(Instant::now());
        }
        if !ied.is_connected() {
            return Err("koneksi ke IED terputus".into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
