//! Browse model IED (event BROWSE): LD -> LN -> DO -> FC, plus CDC yang ditebak dari struktur DO.
//! Asalnya `tools/iec61850-cli` (bin `browse`), di sini satu request `getVariableSpecification`
//! per LN + FC (bukan per DO), jadi IED besar tetap cepat.
//!
//! CDC dari tipe MMS: ENS/ENC tidak bisa dibedakan dari INS/INC (sama-sama integer) -> INS / INC.

use std::collections::BTreeMap;

use serde::Serialize;

use super::client::{fc_from_str, Ied, SpecNode, SpecType};
use crate::error::Result;

/// FC yang bisa dimapping (CHECK `functional_constraint` di `master_datapoint_iec61850s`).
const FCS: [&str; 4] = ["ST", "MX", "CO", "SP"];
/// Kedalaman sub-DO yang dicari kalau CDC DO tidak dikenali (mis. WYE.phsA -> CMV).
const MAX_DEPTH: usize = 3;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BrowseObject {
    pub object_reference: String,
    pub fc: String,
    pub cdc: Option<String>,
}

/// Jalan di thread IED (blocking). Error koneksi -> Err; LN yang tidak punya FC tertentu dilewati.
pub fn browse(ied: &Ied) -> Result<Vec<BrowseObject>> {
    let mut out = Vec::new();
    for ld in ied.logical_devices()? {
        for ln in ied.logical_nodes(&ld)? {
            let ln_ref = format!("{ld}/{ln}");
            let mut specs = Vec::new();
            for fc in FCS {
                match ied.variable_spec(&ln_ref, fc_from_str(fc)?) {
                    Ok(s) => specs.push((fc, s.tree())),
                    // IED menjawab "tidak ada" / spec kosong: LN ini tidak punya FC tsb
                    Err(e) if e.is_server_response() || e.code().is_none() => {}
                    Err(e) => return Err(e),
                }
            }
            out.extend(ln_objects(&ln_ref, &specs));
        }
    }
    Ok(out)
}

/// Satu DO (atau sub-DO) dilihat dari tiap FC: (fc, struktur di FC itu).
type Parts<'a> = Vec<(&'a str, &'a SpecNode)>;
type PartsRef<'a> = [(&'a str, &'a SpecNode)];

/// `specs` = struktur LN per FC (child = DO).
pub fn ln_objects(ln_ref: &str, specs: &[(&str, SpecNode)]) -> Vec<BrowseObject> {
    let mut dos: BTreeMap<&str, Parts> = BTreeMap::new();
    for (fc, ln) in specs {
        for d in &ln.children {
            dos.entry(d.name.as_str()).or_default().push((*fc, d));
        }
    }
    let mut out = Vec::new();
    for (name, parts) in dos {
        let r = format!("{ln_ref}.{name}");
        let found = walk(&r, &parts, 0);
        if found.is_empty() {
            out.extend(objects(&r, &parts, None));
        } else {
            out.extend(found);
        }
    }
    out
}

fn walk(r: &str, parts: &PartsRef, depth: usize) -> Vec<BrowseObject> {
    if let Some(cdc) = classify(parts) {
        return objects(r, parts, Some(cdc));
    }
    if depth >= MAX_DEPTH {
        return Vec::new();
    }
    let mut subs: BTreeMap<&str, Parts> = BTreeMap::new();
    for (fc, n) in parts {
        for c in n.children.iter().filter(|c| c.ty == SpecType::Struct) {
            subs.entry(c.name.as_str()).or_default().push((*fc, c));
        }
    }
    subs.into_iter().flat_map(|(name, p)| walk(&format!("{r}.{name}"), &p, depth + 1)).collect()
}

fn objects(r: &str, parts: &PartsRef, cdc: Option<&str>) -> Vec<BrowseObject> {
    parts
        .iter()
        .map(|(fc, _)| BrowseObject { object_reference: r.to_string(), fc: fc.to_string(), cdc: cdc.map(String::from) })
        .collect()
}

fn attr<'a>(parts: &PartsRef<'a>, fc: &str, name: &str) -> Option<&'a SpecNode> {
    parts.iter().filter(|(f, _)| *f == fc).find_map(|(_, n)| n.children.iter().find(|c| c.name == name))
}

/// Tebak CDC dari atribut wajib IEC 61850-7-3.
fn classify(parts: &PartsRef) -> Option<&'static str> {
    let oper = attr(parts, "CO", "Oper").is_some();
    if let Some(st) = attr(parts, "ST", "stVal") {
        return match (st.ty, oper) {
            (SpecType::Bool, false) => Some("SPS"),
            (SpecType::Bool, true) => Some("SPC"),
            (SpecType::BitString, false) => Some("DPS"),
            (SpecType::BitString, true) => Some("DPC"),
            (SpecType::Int, false) => Some("INS"),
            (SpecType::Int, true) => Some("INC"),
            _ => None,
        };
    }
    if attr(parts, "ST", "general").is_some() {
        return Some(if attr(parts, "ST", "dirGeneral").is_some() { "ACD" } else { "ACT" });
    }
    if attr(parts, "ST", "actVal").is_some() {
        return Some("BCR");
    }
    if oper && attr(parts, "ST", "valWTr").is_some() {
        return Some("BSC");
    }
    if oper && attr(parts, "MX", "mxVal").is_some() {
        return Some("APC");
    }
    if attr(parts, "MX", "mag").is_some() {
        return Some("MV");
    }
    if attr(parts, "MX", "cVal").is_some() {
        return Some("CMV");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(name: &str, ty: SpecType) -> SpecNode {
        SpecNode { name: name.into(), ty, children: vec![] }
    }
    fn st(name: &str, children: Vec<SpecNode>) -> SpecNode {
        SpecNode { name: name.into(), ty: SpecType::Struct, children }
    }
    fn obj(r: &str, fc: &str, cdc: Option<&str>) -> BrowseObject {
        BrowseObject { object_reference: r.into(), fc: fc.into(), cdc: cdc.map(String::from) }
    }

    #[test]
    fn cdc_dari_struktur() {
        let q = || leaf("q", SpecType::BitString);
        let specs = vec![
            ("ST", st("XCBR1", vec![
                st("Pos", vec![leaf("stVal", SpecType::BitString), q()]),
                st("Ind", vec![leaf("stVal", SpecType::Bool), q()]),
                st("Mod", vec![leaf("stVal", SpecType::Int), q()]),
                st("Op", vec![leaf("general", SpecType::Bool)]),
                st("Str", vec![leaf("general", SpecType::Bool), leaf("dirGeneral", SpecType::Int)]),
                st("Cnt", vec![leaf("actVal", SpecType::Int)]),
                st("Aneh", vec![leaf("x", SpecType::Other)]),
            ])),
            ("MX", st("XCBR1", vec![
                st("Amp", vec![st("mag", vec![leaf("f", SpecType::Other)])]),
                st("A", vec![st("phsA", vec![st("cVal", vec![]), q()]), st("phsB", vec![st("cVal", vec![])])]),
            ])),
            ("CO", st("XCBR1", vec![st("Pos", vec![st("Oper", vec![])]), st("Mod", vec![st("Oper", vec![])])])),
        ];
        let r = "LD/XCBR1";
        assert_eq!(
            ln_objects(r, &specs),
            vec![
                obj("LD/XCBR1.A.phsA", "MX", Some("CMV")),
                obj("LD/XCBR1.A.phsB", "MX", Some("CMV")),
                obj("LD/XCBR1.Amp", "MX", Some("MV")),
                obj("LD/XCBR1.Aneh", "ST", None),
                obj("LD/XCBR1.Cnt", "ST", Some("BCR")),
                obj("LD/XCBR1.Ind", "ST", Some("SPS")),
                obj("LD/XCBR1.Mod", "ST", Some("INC")),
                obj("LD/XCBR1.Mod", "CO", Some("INC")),
                obj("LD/XCBR1.Op", "ST", Some("ACT")),
                obj("LD/XCBR1.Pos", "ST", Some("DPC")),
                obj("LD/XCBR1.Pos", "CO", Some("DPC")),
                obj("LD/XCBR1.Str", "ST", Some("ACD")),
            ]
        );
    }
}
