//! RTU_DATAS untuk anggota dataset RCB yang belum punya titik (env `RTU_DATAS_UNMAPPED`, default true), supaya
//! frontend bisa menampilkan value/quality/time anggota yang belum di-mapping (form Add Item). DATAPOINTS tidak dikirim.
//!
//! Saat setup RCB, struktur anggota tanpa titik dibaca (`getVariableSpecification`), lalu anggota dikelompokkan per
//! DO + FC jadi titik virtual (`Point` tanpa IOA / tag). CDC ditebak dengan aturan BROWSE (`browse::classify`):
//! `stVal` -> SPS / DPS / INS, `general` -> ACT / ACD, `actVal` -> BCR, `mag` -> MV, `cVal` -> CMV. Nilai, `q`, `t`
//! diambil dengan decoder titik biasa (`acquisition::decode_unmapped`). Struktur yang tidak dikenal dilewati.

use super::browse::classify;
use super::client::{SpecNode, SpecType};
use super::mapping::default_value_attribute;
use crate::domain::Point;

/// Nama DA yang menandai batas DO: `LD/LN.DO.<da>...` milik DO `LD/LN.DO`. Nama DO / SDO selalu diawali huruf
/// besar (IEC 61850-7-3), jadi tidak tertukar.
const DA_ROOTS: [&str; 8] = ["stVal", "q", "t", "general", "dirGeneral", "actVal", "mag", "cVal"];

/// LN dari reference: `LD/GGIO1.Ind1` -> panjang `LD/GGIO1`.
fn ln_len(reference: &str) -> usize {
    let slash = reference.find('/').unwrap_or(0);
    reference[slash..].find('.').map_or(reference.len(), |i| slash + i)
}

/// (DO, path DA di dalam DO). `LD/X.Pos.stVal` -> (`LD/X.Pos`, [stVal]); sub-DO `LD/M.PhV.phsA.cVal.mag.f` ->
/// (`LD/M.PhV.phsA`, [cVal, mag, f]); anggota level DO `LD/LLN0.Health` -> (anggota itu, []).
fn split_do(member: &str) -> (&str, Vec<&str>) {
    let ln = ln_len(member);
    let segs: Vec<&str> = member.get(ln + 1..).unwrap_or_default().split('.').collect();
    match segs.iter().position(|s| DA_ROOTS.contains(s)) {
        Some(k) if k > 0 => (&member[..ln + 1 + segs[..k].join(".").len()], segs[k..].to_vec()),
        _ => (member, Vec::new()),
    }
}

/// Tempel struktur anggota (`leaf`) ke DO `node` di `path`.
fn insert(node: &mut SpecNode, path: &[&str], leaf: &SpecNode) {
    let Some((first, rest)) = path.split_first() else {
        node.children.extend(leaf.children.iter().cloned());
        return;
    };
    let i = match node.children.iter().position(|c| c.name == *first) {
        Some(i) => i,
        None => {
            node.children.push(SpecNode { name: (*first).into(), ty: SpecType::Struct, children: Vec::new() });
            node.children.len() - 1
        }
    };
    if rest.is_empty() {
        node.children[i] = SpecNode { name: (*first).into(), ..leaf.clone() };
    } else {
        insert(&mut node.children[i], rest, leaf);
    }
}

fn at<'a>(node: &'a SpecNode, path: &str) -> Option<&'a SpecNode> {
    path.split('.').try_fold(node, |n, p| n.children.iter().find(|c| c.name == p))
}

/// (CDC tebakan, atribut nilai) dari struktur DO di satu FC. CDC `""` = ada `stVal` tapi tipenya tidak dikenali.
/// None = struktur tidak dikenal.
pub fn guess(fc: &str, node: &SpecNode) -> Option<(&'static str, String)> {
    let cdc = classify(&[(fc, node)]).or_else(|| at(node, "stVal").map(|_| ""))?;
    let attr = match cdc {
        // mag.f (float) atau mag.i (integer), yang ada di struktur
        "MV" | "CMV" => {
            let base = if cdc == "MV" { "mag" } else { "cVal.mag" };
            ["f", "i"].iter().map(|x| format!("{base}.{x}")).find(|a| at(node, a).is_some())?
        }
        "" => "stVal".to_string(),
        c => default_value_attribute(c)?.to_string(),
    };
    at(node, &attr)?;
    Some((cdc, attr))
}

/// Titik virtual per (DO, FC) dari anggota `(reference, fc, struktur)`, urutan kemunculan pertama di dataset.
pub fn virtual_points<'m>(id_rtu: i32, members: impl IntoIterator<Item = (&'m str, &'m str, &'m SpecNode)>) -> Vec<Point> {
    let mut groups: Vec<(&str, &str, SpecNode)> = Vec::new();
    for (member, fc, tree) in members {
        let (do_ref, path) = split_do(member);
        let i = match groups.iter().position(|(r, f, _)| *r == do_ref && *f == fc) {
            Some(i) => i,
            None => {
                groups.push((do_ref, fc, SpecNode { name: String::new(), ty: SpecType::Struct, children: Vec::new() }));
                groups.len() - 1
            }
        };
        insert(&mut groups[i].2, &path, tree);
    }
    groups
        .into_iter()
        .filter_map(|(r, fc, node)| {
            let (cdc, value_attribute) = guess(fc, &node)?;
            Some(Point {
                id_rtu,
                ioa: 0,
                tag: None,
                object_reference: r.into(),
                fc: fc.into(),
                cdc: cdc.into(),
                value_attribute,
                ctl_model: None,
                interlock_check: false,
                synchro_check: false,
                deadband: None,
                or_cat: None,
            })
        })
        .collect()
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
    fn q() -> SpecNode {
        leaf("q", SpecType::BitString)
    }
    fn t() -> SpecNode {
        leaf("t", SpecType::Other)
    }

    #[test]
    fn do_dari_reference() {
        assert_eq!(split_do("LD/XSWI1.Pos.stVal"), ("LD/XSWI1.Pos", vec!["stVal"]));
        assert_eq!(split_do("LD/XSWI1.Pos.q"), ("LD/XSWI1.Pos", vec!["q"]));
        assert_eq!(split_do("LD/MMXU1.PhV.phsA.cVal.mag.f"), ("LD/MMXU1.PhV.phsA", vec!["cVal", "mag", "f"]));
        assert_eq!(split_do("LD/LLN0.Health"), ("LD/LLN0.Health", vec![]));
        assert_eq!(split_do("LD.x/GGIO1.Ind1.stVal"), ("LD.x/GGIO1.Ind1", vec!["stVal"]));
        assert_eq!(split_do("LD/LLN0"), ("LD/LLN0", vec![]));
    }

    /// Tebakan CDC: SPS, DPS (Dbpos), INS, ACT / ACD (general), MV, CMV; anggota level DO maupun level DA.
    #[test]
    fn tebak_cdc() {
        let m = [
            ("LD/LLN0.Health", "ST", st("Health", vec![leaf("stVal", SpecType::Int), q(), t()])),
            ("LD/GGIO1.Ind1", "ST", st("Ind1", vec![leaf("stVal", SpecType::Bool), q(), t()])),
            ("LD/XSWI1.Pos.stVal", "ST", leaf("stVal", SpecType::BitString)),
            ("LD/XSWI1.Pos.q", "ST", q()),
            ("LD/PTRC1.Op.general", "ST", leaf("general", SpecType::Bool)),
            ("LD/PTOC1.Str", "ST", st("Str", vec![leaf("general", SpecType::Bool), leaf("dirGeneral", SpecType::Int), q()])),
            ("LD/MMXU1.TotW", "MX", st("TotW", vec![st("mag", vec![leaf("f", SpecType::Other)]), q(), t()])),
            ("LD/MMXU1.Hz.mag", "MX", st("mag", vec![leaf("i", SpecType::Int)])),
            ("LD/MMXU1.PhV.phsA.cVal.mag.f", "MX", leaf("f", SpecType::Other)),
            ("LD/MMXU1.PhV.phsA.q", "MX", q()),
            ("LD/CNT1.Cnt", "ST", st("Cnt", vec![leaf("actVal", SpecType::Int), q()])),
        ];
        let pts = virtual_points(9302, m.iter().map(|(r, fc, n)| (*r, *fc, n)));
        let got: Vec<(&str, &str, &str, &str)> =
            pts.iter().map(|p| (p.object_reference.as_str(), p.fc.as_str(), p.cdc.as_str(), p.value_attribute.as_str())).collect();
        assert_eq!(
            got,
            vec![
                ("LD/LLN0.Health", "ST", "INS", "stVal"),
                ("LD/GGIO1.Ind1", "ST", "SPS", "stVal"),
                // stVal + q anggota terpisah -> satu titik
                ("LD/XSWI1.Pos", "ST", "DPS", "stVal"),
                ("LD/PTRC1.Op", "ST", "ACT", "general"),
                ("LD/PTOC1.Str", "ST", "ACD", "general"),
                ("LD/MMXU1.TotW", "MX", "MV", "mag.f"),
                ("LD/MMXU1.Hz", "MX", "MV", "mag.i"),
                ("LD/MMXU1.PhV.phsA", "MX", "CMV", "cVal.mag.f"),
                ("LD/CNT1.Cnt", "ST", "BCR", "actVal"),
            ]
        );
        assert!(pts.iter().all(|p| p.id_rtu == 9302 && p.ioa == 0 && p.tag.is_none()));
    }

    /// Struktur tidak dikenal dilewati tanpa error; stVal bertipe lain -> type "".
    #[test]
    fn struktur_tidak_dikenal_dilewati() {
        let m = [
            ("LD/X1.Foo", "ST", st("Foo", vec![leaf("bar", SpecType::Other)])),
            ("LD/X1.OnlyQ.q", "ST", q()),
            ("LD/X1.Wye", "MX", st("Wye", vec![st("phsA", vec![st("cVal", vec![])])])),
            ("LD/X1.Name", "ST", st("Name", vec![leaf("stVal", SpecType::Other), q()])),
        ];
        let pts = virtual_points(1, m.iter().map(|(r, fc, n)| (*r, *fc, n)));
        assert_eq!(pts.len(), 1, "{pts:?}");
        assert_eq!((pts[0].object_reference.as_str(), pts[0].cdc.as_str(), pts[0].value_attribute.as_str()), ("LD/X1.Name", "", "stVal"));
        assert!(virtual_points(1, Vec::<(&str, &str, &SpecNode)>::new()).is_empty());
    }
}
