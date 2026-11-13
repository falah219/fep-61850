//! Import SCL (ICD/CID/SCD) -> usulan mapping titik + IOA virtual (fase 10).
//!
//! Hasilnya untuk DI-REVIEW manusia (SQL/CSV), bukan langsung dipakai FEP.

use std::collections::HashMap;
use std::fmt::Write as _;

use roxmltree::{Document, Node};

/// Basis IOA per jenis titik (sama dengan seed simulator).
pub const IOA_STATUS: i32 = 1001;
pub const IOA_MEASUREMENT: i32 = 2001;
pub const IOA_CONTROL: i32 = 3001;
/// Maksimal titik per jenis sebelum bertabrakan dengan rentang berikutnya.
const RANGE: i32 = 999;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Status,
    Measurement,
    Control,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SclPoint {
    pub ioa: i32,
    pub tag: String,
    pub object_reference: String,
    pub fc: &'static str,
    pub cdc: String,
    pub kind: Kind,
}

#[derive(Debug, Default)]
pub struct SclIed {
    pub name: String,
    pub points: Vec<SclPoint>,
    /// Instance RCB pertama tiap ReportControl, mis. `LD/LLN0.BR.EventsBRCB01`
    pub rcbs: Vec<String>,
}

struct DoType<'a> {
    cdc: &'a str,
    has_oper: bool,
    default_ctl_model: Option<&'a str>,
    sdos: Vec<(&'a str, &'a str)>,
}

fn children<'a, 'i>(n: Node<'a, 'i>, tag: &'static str) -> impl Iterator<Item = Node<'a, 'i>> {
    n.children().filter(move |c| c.is_element() && c.tag_name().name() == tag)
}

fn val_text<'a>(n: Node<'a, '_>) -> Option<&'a str> {
    children(n, "Val").next().and_then(|v| v.text()).map(str::trim)
}

/// Jenis titik dari CDC. None = tidak didukung (cek SDO di dalamnya).
fn classify(cdc: &str) -> Option<(Kind, &'static str)> {
    Some(match cdc {
        "SPS" | "DPS" | "INS" | "ENS" | "SPC" | "DPC" | "INC" | "BSC" | "BCR" => {
            (if cdc == "BCR" { Kind::Measurement } else { Kind::Status }, "ST")
        }
        "MV" | "CMV" | "APC" => (Kind::Measurement, "MX"),
        _ => return None,
    })
}

fn is_controllable(cdc: &str) -> bool {
    matches!(cdc, "SPC" | "DPC" | "INC" | "APC" | "BSC")
}

/// Parse SCL. `ied_name` wajib kalau file berisi lebih dari satu IED (SCD).
pub fn parse(xml: &str, ied_name: Option<&str>) -> Result<SclIed, String> {
    let doc = Document::parse(xml).map_err(|e| format!("XML tidak valid: {e}"))?;
    let root = doc.root_element();

    let ieds: Vec<_> = children(root, "IED").collect();
    let ied = match ied_name {
        Some(name) => ieds.iter().find(|i| i.attribute("name") == Some(name)).copied(),
        None if ieds.len() == 1 => ieds.first().copied(),
        None => {
            let names: Vec<_> = ieds.iter().filter_map(|i| i.attribute("name")).collect();
            return Err(format!("file berisi {} IED {names:?}, sebutkan nama IED", ieds.len()));
        }
    }
    .ok_or("IED tidak ditemukan di file SCL")?;
    let ied_name = ied.attribute("name").unwrap_or_default().to_string();

    // DataTypeTemplates
    let templates = children(root, "DataTypeTemplates").next().ok_or("DataTypeTemplates tidak ada")?;
    let ln_types: HashMap<&str, Vec<(&str, &str)>> = children(templates, "LNodeType")
        .filter_map(|t| {
            let dos = children(t, "DO").filter_map(|d| Some((d.attribute("name")?, d.attribute("type")?))).collect();
            Some((t.attribute("id")?, dos))
        })
        .collect();
    let do_types: HashMap<&str, DoType> = children(templates, "DOType")
        .filter_map(|t| {
            let das: Vec<_> = children(t, "DA").collect();
            Some((
                t.attribute("id")?,
                DoType {
                    cdc: t.attribute("cdc").unwrap_or_default(),
                    has_oper: das.iter().any(|d| d.attribute("name") == Some("Oper") && d.attribute("fc") == Some("CO")),
                    default_ctl_model: das.iter().find(|d| d.attribute("name") == Some("ctlModel")).and_then(|d| val_text(*d)),
                    sdos: children(t, "SDO").filter_map(|s| Some((s.attribute("name")?, s.attribute("type")?))).collect(),
                },
            ))
        })
        .collect();

    let mut found: Vec<(Kind, &'static str, String, String)> = Vec::new(); // (kind, fc, ref, cdc)
    let mut rcbs = Vec::new();

    for ld in ied.descendants().filter(|n| n.is_element() && n.tag_name().name() == "LDevice") {
        let ld_name = match ld.attribute("ldName") {
            Some(n) => n.to_string(),
            None => format!("{ied_name}{}", ld.attribute("inst").unwrap_or_default()),
        };
        for ln in ld.children().filter(|c| c.is_element() && matches!(c.tag_name().name(), "LN0" | "LN")) {
            let ln_name = format!(
                "{}{}{}",
                ln.attribute("prefix").unwrap_or_default(),
                ln.attribute("lnClass").unwrap_or_default(),
                ln.attribute("inst").unwrap_or_default()
            );
            let ln_ref = format!("{ld_name}/{ln_name}");

            for rc in children(ln, "ReportControl") {
                let Some(name) = rc.attribute("name") else { continue };
                let buffered = rc.attribute("buffered") == Some("true");
                // indexed default true: libIEC61850 menamai instance name01, name02, ...
                let indexed = rc.attribute("indexed") != Some("false");
                let inst = if indexed { format!("{name}01") } else { name.to_string() };
                rcbs.push(format!("{ln_ref}.{}.{inst}", if buffered { "BR" } else { "RP" }));
            }

            let Some(dos) = ln.attribute("lnType").and_then(|t| ln_types.get(t)) else { continue };
            for (do_name, do_type) in dos {
                // ctlModel instance (DOI/DAI) menimpa default dari tipe
                let doi_ctl = children(ln, "DOI")
                    .find(|d| d.attribute("name") == Some(*do_name))
                    .and_then(|d| children(d, "DAI").find(|a| a.attribute("name") == Some("ctlModel")))
                    .and_then(val_text);
                walk_do(&do_types, &format!("{ln_ref}.{do_name}"), do_type, doi_ctl, &mut found);
            }
        }
    }

    Ok(SclIed { points: assign_ioa(&ied_name, found)?, name: ied_name, rcbs })
}

fn walk_do(
    types: &HashMap<&str, DoType>,
    reference: &str,
    type_id: &str,
    doi_ctl_model: Option<&str>,
    out: &mut Vec<(Kind, &'static str, String, String)>,
) {
    let Some(t) = types.get(type_id) else { return };
    match classify(t.cdc) {
        Some((kind, fc)) => {
            out.push((kind, fc, reference.to_string(), t.cdc.to_string()));
            let ctl_model = doi_ctl_model.or(t.default_ctl_model);
            if is_controllable(t.cdc) && t.has_oper && ctl_model != Some("status-only") {
                out.push((Kind::Control, "CO", reference.to_string(), t.cdc.to_string()));
            }
        }
        // CDC komposit (WYE, DEL, ...): turun ke SDO (mis. PhV.phsA bertipe CMV)
        None => {
            for (sdo, sdo_type) in &t.sdos {
                walk_do(types, &format!("{reference}.{sdo}"), sdo_type, None, out);
            }
        }
    }
}

/// Tag usulan: reference dengan `/` & `.` jadi `_`, titik kontrol diberi akhiran `_CO`.
fn make_tag(reference: &str, kind: Kind) -> String {
    let base: String = reference
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect();
    if kind == Kind::Control { format!("{base}_CO") } else { base }
}

fn assign_ioa(ied: &str, found: Vec<(Kind, &'static str, String, String)>) -> Result<Vec<SclPoint>, String> {
    let mut next = [IOA_STATUS, IOA_MEASUREMENT, IOA_CONTROL];
    let mut points = Vec::with_capacity(found.len());
    for (kind, fc, reference, cdc) in found {
        let slot = &mut next[kind as usize];
        let base = [IOA_STATUS, IOA_MEASUREMENT, IOA_CONTROL][kind as usize];
        if *slot > base + RANGE - 1 {
            return Err(format!("IED {ied}: titik {kind:?} lebih dari {RANGE}, rentang IOA tidak cukup"));
        }
        points.push(SclPoint { ioa: *slot, tag: make_tag(&reference, kind), object_reference: reference, fc, cdc, kind });
        *slot += 1;
    }
    points.sort_by_key(|p| p.ioa);
    Ok(points)
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// SQL usulan (pola sama dengan migrations/seed_simulator.sql).
pub fn to_sql(ied: &SclIed, id_rtu: i32, source: &str) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "-- Dibuat oleh scl-import dari {source}, IED {}, id_rtu {id_rtu}.", ied.name);
    s.push_str("-- USULAN: REVIEW dulu (tag, titik yang tidak perlu) sebelum dijalankan. Hanya DB lokal/dev.\n");
    s.push_str("-- Baris iec61850s untuk RTU ini dibuat lewat form GRITA, tidak di sini.\n\nBEGIN;\n\n");
    s.push_str("CREATE TEMP TABLE scl_points (ioa INTEGER, tag TEXT, object_reference TEXT, fc TEXT, cdc TEXT) ON COMMIT DROP;\n\n");
    if !ied.points.is_empty() {
        s.push_str("INSERT INTO scl_points VALUES\n");
        let rows: Vec<String> = ied
            .points
            .iter()
            .map(|p| format!("    ({}, {}, {}, '{}', '{}')", p.ioa, sql_str(&p.tag), sql_str(&p.object_reference), p.fc, p.cdc))
            .collect();
        s.push_str(&rows.join(",\n"));
        s.push_str(";\n\n");
    }
    let _ = writeln!(
        s,
        "INSERT INTO master_datapoints (id, id_rtu, ioa, tag, protocol, is_deleted, created_at, updated_at)\n\
         SELECT gen_random_uuid(), {id_rtu}, ioa, tag, 'IEC61850', FALSE, now(), now() FROM scl_points;\n\n\
         INSERT INTO master_datapoint_iec61850s (id_master_datapoint, object_reference, functional_constraint, cdc)\n\
         SELECT md.id, s.object_reference, s.fc, s.cdc\n\
         FROM scl_points s\n\
         JOIN master_datapoints md\n  \
           ON md.id_rtu = {id_rtu} AND md.ioa = s.ioa AND md.protocol = 'IEC61850' AND md.is_deleted = FALSE;\n"
    );
    if !ied.rcbs.is_empty() {
        s.push_str("\n-- RCB yang ada di SCL (instance pertama). Aktifkan yang dipakai saja (hapus `--`).\n");
        s.push_str("-- trg_ops/intg_pd_ms NULL = pakai setting IED.\n");
        for r in &ied.rcbs {
            let _ = writeln!(s, "-- INSERT INTO iec61850_report_controls (id_rtu, rcb_reference) VALUES ({id_rtu}, {});", sql_str(r));
        }
    }
    s.push_str("\nCOMMIT;\n");
    s
}

pub fn to_csv(ied: &SclIed, id_rtu: i32) -> String {
    let mut s = String::from("id_rtu,ioa,tag,object_reference,fc,cdc\n");
    for p in &ied.points {
        let _ = writeln!(s, "{id_rtu},{},{},{},{},{}", p.ioa, p.tag, p.object_reference, p.fc, p.cdc);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCL: &str = r#"<?xml version="1.0"?>
<SCL xmlns="http://www.iec.ch/61850/2003/SCL">
  <IED name="BAY1">
    <AccessPoint name="AP1"><Server><LDevice inst="LD0">
      <LN0 lnClass="LLN0" lnType="LLN0T" inst="">
        <ReportControl name="brcbST" buffered="true" datSet="ST"><RptEnabled max="2"/></ReportControl>
        <ReportControl name="urcbMX" indexed="false" datSet="MX"/>
      </LN0>
      <LN lnClass="XCBR" lnType="XCBRT" inst="1" prefix="">
        <DOI name="Pos"><DAI name="ctlModel"><Val>sbo-with-enhanced-security</Val></DAI></DOI>
      </LN>
      <LN lnClass="CSWI" lnType="CSWIT" inst="1" prefix="Q0"/>
      <LN lnClass="MMXU" lnType="MMXUT" inst="1" prefix=""/>
    </LDevice></Server></AccessPoint>
  </IED>
  <DataTypeTemplates>
    <LNodeType id="LLN0T" lnClass="LLN0"><DO name="Mod" type="ENC1"/><DO name="Health" type="ENS1"/></LNodeType>
    <LNodeType id="XCBRT" lnClass="XCBR"><DO name="Pos" type="DPC_ST"/></LNodeType>
    <LNodeType id="CSWIT" lnClass="CSWI"><DO name="Pos" type="DPC_CO"/></LNodeType>
    <LNodeType id="MMXUT" lnClass="MMXU"><DO name="TotW" type="MV1"/><DO name="PhV" type="WYE1"/></LNodeType>
    <DOType id="ENC1" cdc="ENC"><DA name="stVal" fc="ST"/></DOType>
    <DOType id="ENS1" cdc="ENS"><DA name="stVal" fc="ST"/></DOType>
    <DOType id="DPC_ST" cdc="DPC">
      <DA name="stVal" fc="ST"/><DA name="Oper" fc="CO"/>
      <DA name="ctlModel" fc="CF"><Val>status-only</Val></DA>
    </DOType>
    <DOType id="DPC_CO" cdc="DPC">
      <DA name="stVal" fc="ST"/><DA name="Oper" fc="CO"/>
      <DA name="ctlModel" fc="CF"><Val>status-only</Val></DA>
    </DOType>
    <DOType id="MV1" cdc="MV"><DA name="mag" fc="MX"/></DOType>
    <DOType id="WYE1" cdc="WYE"><SDO name="phsA" type="CMV1"/><SDO name="phsB" type="CMV1"/></DOType>
    <DOType id="CMV1" cdc="CMV"><DA name="cVal" fc="MX"/></DOType>
  </DataTypeTemplates>
</SCL>"#;

    #[test]
    fn mapping_dan_ioa() {
        let ied = parse(SCL, None).unwrap();
        assert_eq!(ied.name, "BAY1");
        let got: Vec<(i32, &str, &str, &str)> =
            ied.points.iter().map(|p| (p.ioa, p.object_reference.as_str(), p.fc, p.cdc.as_str())).collect();
        assert_eq!(
            got,
            vec![
                (1001, "BAY1LD0/LLN0.Health", "ST", "ENS"),
                (1002, "BAY1LD0/XCBR1.Pos", "ST", "DPC"),
                (1003, "BAY1LD0/Q0CSWI1.Pos", "ST", "DPC"),
                (2001, "BAY1LD0/MMXU1.TotW", "MX", "MV"),
                (2002, "BAY1LD0/MMXU1.PhV.phsA", "MX", "CMV"),
                (2003, "BAY1LD0/MMXU1.PhV.phsB", "MX", "CMV"),
                // XCBR1.Pos: ctlModel instance (sbo-enhanced) menimpa status-only -> bisa dikontrol.
                (3001, "BAY1LD0/XCBR1.Pos", "CO", "DPC"),
            ]
        );
        // Q0CSWI1.Pos status-only -> tidak ada titik kontrol. Mod (ENC) tidak didukung.
        assert_eq!(ied.points.last().map(|p| p.tag.as_str()), Some("BAY1LD0_XCBR1_Pos_CO"));
    }

    #[test]
    fn rcb_instance() {
        let ied = parse(SCL, None).unwrap();
        assert_eq!(ied.rcbs, vec!["BAY1LD0/LLN0.BR.brcbST01", "BAY1LD0/LLN0.RP.urcbMX"]);
    }

    #[test]
    fn scd_banyak_ied_wajib_nama() {
        let two = SCL.replace("</IED>", "</IED><IED name=\"BAY2\"/>");
        assert!(parse(&two, None).unwrap_err().contains("BAY2"));
        assert_eq!(parse(&two, Some("BAY1")).unwrap().name, "BAY1");
        assert!(parse(&two, Some("X")).is_err());
    }

    #[test]
    fn sql_escape_dan_isi() {
        let ied = parse(SCL, None).unwrap();
        let sql = to_sql(&ied, 9001, "uji.scd");
        assert!(sql.contains("(3001, 'BAY1LD0_XCBR1_Pos_CO', 'BAY1LD0/XCBR1.Pos', 'CO', 'DPC')"));
        assert!(sql.contains("-- INSERT INTO iec61850_report_controls (id_rtu, rcb_reference) VALUES (9001, 'BAY1LD0/LLN0.BR.brcbST01');"));
        assert_eq!(sql_str("a'b"), "'a''b'");
        assert!(to_csv(&ied, 9001).starts_with("id_rtu,ioa,tag,object_reference,fc,cdc\n9001,1001,"));
    }
}
