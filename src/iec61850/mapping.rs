//! Mapping titik IEC 61850 <-> IOA virtual (docs/IEC61850_PLAN.md bagian 2.1).
//!
//! Dimuat sekali saat start dari `master_datapoints` + `master_datapoint_iec61850s`, lalu dipakai
//! untuk menerjemahkan setiap data yang masuk: (id_rtu, reference, FC) -> (ioa, tag, ...).

use std::collections::HashMap;

use crate::domain::Point;

/// Atribut nilai default per CDC (dipakai kalau `value_attribute` NULL).
pub fn default_value_attribute(cdc: &str) -> Option<&'static str> {
    Some(match cdc {
        "SPS" | "DPS" | "INS" | "ENS" | "ENC" | "SPC" | "DPC" | "INC" => "stVal",
        // Proteksi: trip (ACT) / start dengan arah (ACD)
        "ACT" | "ACD" => "general",
        "MV" => "mag.f",
        "CMV" => "cVal.mag.f",
        "BCR" => "actVal",
        "APC" => "mxVal.f",
        "BSC" => "valWTr.posVal",
        _ => return None,
    })
}

/// Pisahkan `LD/LN.DO.da[FC]` -> (`LD/LN.DO.da`, `FC`).
pub fn split_fc(member: &str) -> Option<(&str, &str)> {
    let (r, rest) = member.rsplit_once('[')?;
    Some((r, rest.strip_suffix(']')?))
}

#[derive(Default)]
pub struct PointMap {
    points: Vec<Point>,
    by_ioa: HashMap<(i32, i32), usize>,
    /// id_rtu -> object_reference -> indeks titik (satu DO bisa punya beberapa FC, mis. ST + CO)
    by_ref: HashMap<i32, HashMap<String, Vec<usize>>>,
}

impl PointMap {
    pub fn insert(&mut self, p: Point) -> Result<(), String> {
        if self.by_ioa.contains_key(&(p.id_rtu, p.ioa)) {
            return Err(format!("IOA {} dipakai dua kali di RTU {}", p.ioa, p.id_rtu));
        }
        let refs = self.by_ref.entry(p.id_rtu).or_default();
        let slot = refs.entry(p.object_reference.clone()).or_default();
        if slot.iter().any(|&i| self.points[i].fc == p.fc) {
            return Err(format!(
                "{} [{}] dipakai dua kali di RTU {}",
                p.object_reference, p.fc, p.id_rtu
            ));
        }
        let i = self.points.len();
        slot.push(i);
        self.by_ioa.insert((p.id_rtu, p.ioa), i);
        self.points.push(p);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Point> {
        self.points.iter()
    }

    /// Semua titik milik satu RTU.
    // ponytail: scan linear, hanya dipakai saat setup koneksi; buat indeks per RTU kalau dipanggil sering.
    pub fn rtu_points(&self, id_rtu: i32) -> impl Iterator<Item = &Point> {
        self.points.iter().filter(move |p| p.id_rtu == id_rtu)
    }

    /// Untuk command: (id_rtu, ioa) -> titik.
    pub fn by_ioa(&self, id_rtu: i32, ioa: i32) -> Option<&Point> {
        self.by_ioa.get(&(id_rtu, ioa)).map(|&i| &self.points[i])
    }

    /// Untuk data masuk: reference apa pun (DO, SDO, atau DA) -> titik + sisa path atribut.
    /// Contoh: `LD/GGIO1.AnIn1.mag.f` [MX] -> (titik AnIn1, `"mag.f"`). Prefix terpanjang yang menang.
    pub fn by_reference<'a>(&self, id_rtu: i32, reference: &'a str, fc: &str) -> Option<(&Point, &'a str)> {
        let refs = self.by_ref.get(&id_rtu)?;
        let ln_end = reference.find('/')?;
        let mut prefix = reference;
        loop {
            if let Some(idx) = refs.get(prefix) {
                if let Some(&i) = idx.iter().find(|&&i| self.points[i].fc == fc) {
                    let rest = reference[prefix.len()..].trim_start_matches('.');
                    return Some((&self.points[i], rest));
                }
            }
            // potong satu level dari belakang, tapi jangan sampai melewati LN (`LD/LN`)
            let cut = prefix.rfind('.')?;
            if cut <= ln_end || !prefix[ln_end..cut].contains('.') {
                return None;
            }
            prefix = &prefix[..cut];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(ioa: i32, r: &str, fc: &str, cdc: &str) -> Point {
        Point {
            id_rtu: 1,
            ioa,
            tag: None,
            object_reference: r.into(),
            fc: fc.into(),
            cdc: cdc.into(),
            value_attribute: default_value_attribute(cdc).unwrap_or_default().into(),
            ctl_model: None,
            interlock_check: false,
            synchro_check: false,
            deadband: None,
        }
    }

    fn map() -> PointMap {
        let mut m = PointMap::default();
        m.insert(pt(1001, "LD/GGIO1.SPCSO1", "ST", "SPC")).unwrap();
        m.insert(pt(3001, "LD/GGIO1.SPCSO1", "CO", "SPC")).unwrap();
        m.insert(pt(2001, "LD/GGIO1.AnIn1", "MX", "MV")).unwrap();
        m.insert(pt(2101, "LD/MMXU1.PhV.phsA", "MX", "CMV")).unwrap();
        m
    }

    #[test]
    fn default_attr_per_cdc() {
        assert_eq!(default_value_attribute("DPS"), Some("stVal"));
        assert_eq!(default_value_attribute("MV"), Some("mag.f"));
        assert_eq!(default_value_attribute("CMV"), Some("cVal.mag.f"));
        assert_eq!(default_value_attribute("BCR"), Some("actVal"));
        assert_eq!(default_value_attribute("ACT"), Some("general"));
        assert_eq!(default_value_attribute("ACD"), Some("general"));
        assert_eq!(default_value_attribute("ENC"), Some("stVal"));
        assert_eq!(default_value_attribute("XYZ"), None);
    }

    #[test]
    fn pisah_fc() {
        assert_eq!(split_fc("LD/GGIO1.AnIn1.mag.f[MX]"), Some(("LD/GGIO1.AnIn1.mag.f", "MX")));
        assert_eq!(split_fc("LD/GGIO1.AnIn1"), None);
    }

    #[test]
    fn lookup_ioa() {
        let m = map();
        assert_eq!(m.by_ioa(1, 3001).map(|p| p.fc.as_str()), Some("CO"));
        assert!(m.by_ioa(1, 9999).is_none());
        assert!(m.by_ioa(2, 1001).is_none());
    }

    #[test]
    fn lookup_reference_do_dan_da() {
        let m = map();
        let (p, rest) = m.by_reference(1, "LD/GGIO1.SPCSO1", "ST").unwrap();
        assert_eq!((p.ioa, rest), (1001, ""));
        let (p, rest) = m.by_reference(1, "LD/GGIO1.SPCSO1.stVal", "ST").unwrap();
        assert_eq!((p.ioa, rest), (1001, "stVal"));
        let (p, rest) = m.by_reference(1, "LD/GGIO1.AnIn1.mag.f", "MX").unwrap();
        assert_eq!((p.ioa, rest), (2001, "mag.f"));
        // SDO: prefix terpanjang
        let (p, rest) = m.by_reference(1, "LD/MMXU1.PhV.phsA.cVal.mag.f", "MX").unwrap();
        assert_eq!((p.ioa, rest), (2101, "cVal.mag.f"));
    }

    #[test]
    fn lookup_reference_gagal() {
        let m = map();
        assert!(m.by_reference(1, "LD/GGIO1.SPCSO1.stVal", "MX").is_none()); // FC beda
        assert!(m.by_reference(1, "LD/GGIO1.Ind1.stVal", "ST").is_none()); // tidak dimapping
        assert!(m.by_reference(2, "LD/GGIO1.AnIn1", "MX").is_none()); // RTU lain
        assert!(m.by_reference(1, "LD/GGIO1", "MX").is_none());
        assert!(m.by_reference(1, "tanpa-slash", "MX").is_none());
    }

    #[test]
    fn tolak_duplikat() {
        let mut m = map();
        assert!(m.insert(pt(1001, "LD/GGIO1.Ind1", "ST", "SPS")).is_err()); // IOA sama
        assert!(m.insert(pt(1999, "LD/GGIO1.SPCSO1", "ST", "SPC")).is_err()); // ref+FC sama
        assert_eq!(m.len(), 4);
    }
}
