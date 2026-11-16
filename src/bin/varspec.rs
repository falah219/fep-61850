//! Debug getVariableSpecification satu objek (mis. IED yang BROWSE-nya gagal `malformed-message`).
//!
//! Pemakaian: varspec <host> <port> <LD/LN[.DO]> [FC ...]
//!   FC default: ST MX CO CF SP. Env MMS_MAX_PDU_SIZE ikut dipakai (sama dengan FEP).
//! Mencetak max PDU hasil negosiasi, lalu per FC: struktur (nama, tipe) atau error-nya, dan nama
//! GetNameList (fallback browse FEP) untuk objek itu.

use fep_iec61850::iec61850::client::{fc_from_str, ConnectParams, Ied, SpecNode};

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn print_tree(n: &SpecNode, depth: usize) {
    println!("{:indent$}{} : {:?}", "", n.name, n.ty, indent = depth * 2);
    for c in &n.children {
        print_tree(c, depth + 1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [host, port, reference, fcs @ ..] = args.as_slice() else {
        return Err("pemakaian: varspec <host> <port> <LD/LN[.DO]> [FC ...]".into());
    };
    let port: i32 = port.parse().map_err(|_| format!("port tidak valid: {port}"))?;
    let params = ConnectParams { host, port, local_ip: None, connect_timeout_ms: 10_000, request_timeout_ms: 10_000, osi: None };
    let ied = Ied::connect(&params).map_err(|e| format!("konek {host}:{port}: {e}"))?;
    println!("terkoneksi {host}:{port}, max PDU {}", ied.max_pdu_size());

    let default_fcs = ["ST", "MX", "CO", "CF", "SP"].map(String::from);
    let fcs = if fcs.is_empty() { &default_fcs[..] } else { fcs };
    for fc in fcs {
        let code = fc_from_str(fc).map_err(|e| e.to_string())?;
        println!("\n== getVariableSpecification {reference} [{fc}]");
        match ied.variable_spec(reference, code) {
            Ok(spec) => print_tree(&spec.tree(), 0),
            Err(e) => println!("ERROR: {e}"),
        }
    }

    // GetNameList: nama MMS `LN$FC$DO$...` yang diawali objek ini
    let (ld, rest) = reference.split_once('/').ok_or("reference harus LD/LN[.DO]")?;
    let mut segs = rest.split('.');
    let ln = segs.next().unwrap_or_default();
    let tail: Vec<&str> = segs.collect();
    println!("\n== GetNameList {ld} (fallback browse)");
    match ied.logical_device_variables(ld) {
        Ok(names) => {
            let hits = names.iter().filter(|n| {
                let p: Vec<&str> = n.split('$').collect();
                p.first() == Some(&ln) && p.len() >= 2 + tail.len() && p[2..2 + tail.len()] == tail[..]
            });
            for n in hits {
                println!("{n}");
            }
        }
        Err(e) => println!("ERROR: {e}"),
    }
    Ok(())
}
