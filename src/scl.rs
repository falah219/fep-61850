//! Parser SCL (ICD/CID/SCD), murni di memori (roxmltree: tanpa DTD / external entity).
//!
//! - `parse`: usulan mapping titik + IOA virtual (fase 10, bin `scl-import`), untuk DI-REVIEW manusia (SQL/CSV).
//! - `catalog`: katalog model IED untuk event SCL_IMPORT, format SAMA dengan hasil BROWSE
//!   (`BrowseObject` / `RcbInfo` / `DatasetMember`: `LD/LN.DO`, `LD/LLN0.BR.<rcb>`, `LD/LLN0.<dataset>`).

use std::collections::HashMap;
use std::fmt::Write as _;

use roxmltree::{Document, Node};
use serde::{Deserialize, Serialize};

use crate::iec61850::browse::{ctl_model_name, BrowseError, BrowseObject, BrowseResult, ControlInfo, DatasetMember, RcbInfo, FCS, MAX_DEPTH};
use crate::iec61850::mapper::OPT_FLDS;

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

fn is(n: Node, tag: &str) -> bool {
    n.is_element() && n.tag_name().name() == tag
}

/// Anak `tag` dengan atribut `name` = `name`.
fn named<'a, 'i>(n: Node<'a, 'i>, tag: &'static str, name: &str) -> Option<Node<'a, 'i>> {
    children(n, tag).find(|c| c.attribute("name") == Some(name))
}

/// Nilai `DOI/DAI[name]/Val`.
fn dai<'a>(doi: Node<'a, '_>, name: &str) -> Option<&'a str> {
    named(doi, "DAI", name).and_then(val_text)
}

/// File rusak, IED tidak ada / harus dipilih.
#[derive(Debug, Clone, PartialEq)]
pub struct SclError {
    pub message: String,
    /// Pilihan IED (SCD lebih dari satu IED tanpa `ied_name`, atau `ied_name` salah)
    pub ied_names: Option<Vec<IedRef>>,
}

impl SclError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), ied_names: None }
    }
}

impl std::fmt::Display for SclError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)?;
        if let Some(ieds) = &self.ied_names {
            let names: Vec<&str> = ieds.iter().map(|i| i.name.as_str()).collect();
            write!(f, " ({})", names.join(", "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IedRef {
    pub name: String,
    pub ip: Option<String>,
}

/// XML valid dengan root `<SCL>`. roxmltree menolak DTD (tidak ada entity eksternal).
fn open(xml: &str) -> Result<Document<'_>, SclError> {
    let invalid = |d: String| SclError::new(format!("File bukan SCL yang valid: {d}"));
    let doc = Document::parse(xml).map_err(|e| invalid(e.to_string()))?;
    let root = doc.root_element().tag_name().name();
    if root != "SCL" {
        return Err(invalid(format!("root <{root}>, bukan <SCL>")));
    }
    Ok(doc)
}

/// `Communication/SubNetwork/ConnectedAP[iedName]/Address/P[type="IP"]`.
fn ied_ip(root: Node, ied: &str) -> Option<String> {
    root.descendants()
        .filter(|n| is(*n, "ConnectedAP") && n.attribute("iedName") == Some(ied))
        .flat_map(|ap| children(ap, "Address").flat_map(|a| children(a, "P")))
        .find(|p| p.attribute("type") == Some("IP"))
        .and_then(|p| p.text())
        .map(|t| t.trim().to_string())
}

fn select_ied<'a, 'i>(root: Node<'a, 'i>, name: Option<&str>) -> Result<Node<'a, 'i>, SclError> {
    let ieds: Vec<_> = children(root, "IED").collect();
    let choices = || {
        let refs = ieds.iter().filter_map(|i| i.attribute("name")).map(|n| IedRef { name: n.into(), ip: ied_ip(root, n) });
        Some(refs.collect::<Vec<_>>())
    };
    match name {
        Some(n) => ieds
            .iter()
            .copied()
            .find(|i| i.attribute("name") == Some(n))
            .ok_or_else(|| SclError { message: format!("IED {n} tidak ada di file"), ied_names: choices() }),
        None if ieds.len() == 1 => Ok(ieds[0]),
        None if ieds.is_empty() => Err(SclError::new("File SCL tidak berisi IED")),
        None => Err(SclError { message: "Pilih IED".into(), ied_names: choices() }),
    }
}

fn ldevices<'a, 'i>(ied: Node<'a, 'i>) -> impl Iterator<Item = Node<'a, 'i>> {
    ied.descendants().filter(|n| is(*n, "LDevice"))
}

fn lns<'a, 'i>(ld: Node<'a, 'i>) -> impl Iterator<Item = Node<'a, 'i>> {
    ld.children().filter(|c| is(*c, "LN0") || is(*c, "LN"))
}

/// Atribut `ldName` kalau ada, kalau tidak `<IED name><LDevice inst>`.
fn ld_name(ied: &str, ld: Node) -> String {
    ld.attribute("ldName").map_or_else(|| format!("{ied}{}", ld.attribute("inst").unwrap_or_default()), String::from)
}

/// prefix + lnClass + inst
fn ln_name(ln: Node) -> String {
    let a = |k: &'static str| ln.attribute(k).unwrap_or_default();
    format!("{}{}{}", a("prefix"), a("lnClass"), a("inst"))
}

/// (`BR`/`RP`, nama instance). indexed (default true): libIEC61850 menamai instance name01..name<max>
/// (`RptEnabled max`, default 1); `indexed="false"`: name apa adanya.
fn rcb_instances(rc: Node, name: &str) -> (&'static str, Vec<String>) {
    let tag = if rc.attribute("buffered") == Some("true") { "BR" } else { "RP" };
    if rc.attribute("indexed") == Some("false") {
        return (tag, vec![name.to_string()]);
    }
    let max = children(rc, "RptEnabled").next().and_then(|r| num::<u32>(r, "max")).unwrap_or(1).max(1);
    (tag, (1..=max).map(|i| format!("{name}{i:02}")).collect())
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
    let doc = open(xml).map_err(|e| e.to_string())?;
    let root = doc.root_element();
    let ied = select_ied(root, ied_name).map_err(|e| e.to_string())?;
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

    for ld in ldevices(ied) {
        let ld_name = ld_name(&ied_name, ld);
        for ln in lns(ld) {
            let ln_ref = format!("{ld_name}/{}", ln_name(ln));

            for rc in children(ln, "ReportControl") {
                let Some(name) = rc.attribute("name") else { continue };
                let (tag, instances) = rcb_instances(rc, name);
                rcbs.push(format!("{ln_ref}.{tag}.{}", instances[0]));
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

// ---------------------------------------------------------------- katalog (SCL_IMPORT)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IedInfo {
    pub name: String,
    pub manufacturer: Option<String>,
    #[serde(rename = "type")]
    pub ied_type: Option<String>,
    pub config_version: Option<String>,
    pub ip: Option<String>,
}

/// `LLN0.NamPlt.configRev` satu LD.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigRev {
    pub ld: String,
    pub reference: String,
    pub value: String,
}

#[derive(Debug, PartialEq)]
pub struct SclCatalog {
    pub ied: IedInfo,
    pub config_revs: Vec<ConfigRev>,
    /// objects / rcbs / errors, format sama dengan BROWSE
    pub result: BrowseResult,
}

struct Types<'a, 'i> {
    ln: HashMap<&'a str, Node<'a, 'i>>,
    dos: HashMap<&'a str, Node<'a, 'i>>,
}

fn type_index<'a, 'i>(tpl: Option<Node<'a, 'i>>, tag: &'static str) -> HashMap<&'a str, Node<'a, 'i>> {
    tpl.into_iter().flat_map(move |t| children(t, tag)).filter_map(|n| Some((n.attribute("id")?, n))).collect()
}

fn type_error(reference: &str, kind: &str, id: &str) -> BrowseError {
    BrowseError { reference: reference.into(), error: format!("{kind} {id:?} tidak ada di DataTypeTemplates") }
}

/// Katalog model IED dari SCL (event SCL_IMPORT). Tipe yang tidak ada -> objek dilewati + `errors`.
pub fn catalog(xml: &str, ied_name: Option<&str>) -> Result<SclCatalog, SclError> {
    let doc = open(xml)?;
    let root = doc.root_element();
    let ied = select_ied(root, ied_name)?;
    let name = ied.attribute("name").unwrap_or_default();
    let attr = |k: &'static str| ied.attribute(k).map(String::from);
    let info = IedInfo {
        name: name.into(),
        manufacturer: attr("manufacturer"),
        ied_type: attr("type"),
        config_version: attr("configVersion"),
        ip: ied_ip(root, name),
    };
    let tpl = children(root, "DataTypeTemplates").next();
    let types = Types { ln: type_index(tpl, "LNodeType"), dos: type_index(tpl, "DOType") };
    let lds: HashMap<&str, String> = ldevices(ied).map(|ld| (ld.attribute("inst").unwrap_or_default(), ld_name(name, ld))).collect();

    let mut out = BrowseResult::default();
    let mut config_revs = Vec::new();
    for ld in ldevices(ied) {
        let ld_ref = ld_name(name, ld);
        for ln in lns(ld) {
            let ln_ref = format!("{ld_ref}/{}", ln_name(ln));
            ln_objects(&types, ln, &ln_ref, &mut out);
            reports(ln, &ln_ref, name, &lds, &mut out);
            if is(ln, "LN0") {
                if let Some(v) = named(ln, "DOI", "NamPlt").and_then(|d| dai(d, "configRev")) {
                    config_revs.push(ConfigRev { ld: ld_ref.clone(), reference: format!("{ln_ref}.NamPlt.configRev"), value: v.into() });
                }
            }
        }
    }
    Ok(SclCatalog { ied: info, config_revs, result: out })
}

/// DO satu LN, urut nama (sama dengan browse).
fn ln_objects(types: &Types, ln: Node, ln_ref: &str, out: &mut BrowseResult) {
    let ln_type = ln.attribute("lnType").unwrap_or_default();
    let Some(t) = types.ln.get(ln_type) else {
        return out.errors.push(type_error(ln_ref, "LNodeType", ln_type));
    };
    let mut dos: Vec<_> = children(*t, "DO").filter_map(|d| Some((d.attribute("name")?, d.attribute("type")?))).collect();
    dos.sort_by_key(|(n, _)| *n);
    for (do_name, do_type) in dos {
        catalog_do(types, &format!("{ln_ref}.{do_name}"), do_type, named(ln, "DOI", do_name), 0, out);
    }
}

/// DO yang punya DA di FC mapping -> satu objek per FC (urutan FCS), CDC dari DOType. Tidak punya -> turun
/// ke SDO (mis. WYE.phsA bertipe CMV), maks MAX_DEPTH, sama dengan browse. `inst` = DOI/SDI instance.
fn catalog_do(types: &Types, r: &str, type_id: &str, inst: Option<Node>, depth: usize, out: &mut BrowseResult) {
    let Some(t) = types.dos.get(type_id) else {
        return out.errors.push(type_error(r, "DOType", type_id));
    };
    let das: Vec<Node> = children(*t, "DA").collect();
    let fcs: Vec<&str> = FCS.iter().copied().filter(|fc| das.iter().any(|d| d.attribute("fc") == Some(*fc))).collect();
    if !fcs.is_empty() {
        let cdc = t.attribute("cdc").filter(|c| !c.is_empty()).map(String::from);
        for fc in fcs {
            let control = (fc == "CO").then(|| control_info(&das, inst));
            out.objects.push(BrowseObject { object_reference: r.into(), fc: fc.into(), cdc: cdc.clone(), control });
        }
        return;
    }
    if depth >= MAX_DEPTH {
        return;
    }
    let mut sdos: Vec<_> = children(*t, "SDO").filter_map(|d| Some((d.attribute("name")?, d.attribute("type")?))).collect();
    sdos.sort_by_key(|(n, _)| *n);
    for (sdo, sdo_type) in sdos {
        catalog_do(types, &format!("{r}.{sdo}"), sdo_type, inst.and_then(|i| named(i, "SDI", sdo)), depth + 1, out);
    }
}

/// Nilai DA: instance (DOI/DAI) menimpa `Val` default di DOType.
fn da_value<'a>(das: &[Node<'a, '_>], inst: Option<Node<'a, '_>>, name: &str) -> Option<&'a str> {
    inst.and_then(|i| dai(i, name)).or_else(|| das.iter().find(|d| d.attribute("name") == Some(name)).and_then(|d| val_text(*d)))
}

fn control_info(das: &[Node], inst: Option<Node>) -> ControlInfo {
    let has = |n: &str| das.iter().any(|d| d.attribute("name") == Some(n) && d.attribute("fc") == Some("CO"));
    ControlInfo {
        // teks enum SCL = teks kolom ctl_model; angka (jarang) diubah dengan tabel yang sama dengan browse
        ctl_model: da_value(das, inst, "ctlModel").map(|v| v.parse().map_or_else(|_| v.to_string(), ctl_model_name)),
        sbo_timeout_ms: da_value(das, inst, "sboTimeout").and_then(|v| v.parse().ok()),
        sbow: has("SBOw"),
        oper: has("Oper"),
        cancel: has("Cancel"),
    }
}

/// `true`/`1` -> true, tidak ada -> `default`.
fn flag(n: Node, k: &str, default: bool) -> bool {
    n.attribute(k).map_or(default, |v| v == "true" || v == "1")
}

fn num<T: std::str::FromStr>(n: Node, k: &str) -> Option<T> {
    n.attribute(k).and_then(|v| v.trim().parse().ok())
}

/// FCDA -> `{object_reference, fc}` (`LD/LN.DO[.DA]`, sama dengan anggota dataset browse).
fn fcda(f: Node, ied: &str, lds: &HashMap<&str, String>) -> DatasetMember {
    let a = |k: &'static str| f.attribute(k).unwrap_or_default();
    let ld = lds.get(a("ldInst")).cloned().unwrap_or_else(|| format!("{ied}{}", a("ldInst")));
    let mut r = format!("{ld}/{}{}{}.{}", a("prefix"), a("lnClass"), a("lnInst"), a("doName"));
    if !a("daName").is_empty() {
        r.push('.');
        r.push_str(a("daName"));
    }
    DatasetMember { object_reference: r, fc: a("fc").into() }
}

/// DataSet + ReportControl satu LN -> `rcbs` (satu per instance).
fn reports(ln: Node, ln_ref: &str, ied: &str, lds: &HashMap<&str, String>, out: &mut BrowseResult) {
    let datasets: HashMap<&str, Vec<DatasetMember>> = children(ln, "DataSet")
        .filter_map(|ds| Some((ds.attribute("name")?, children(ds, "FCDA").map(|f| fcda(f, ied, lds)).collect::<Vec<_>>())))
        .collect();
    for rc in children(ln, "ReportControl") {
        let Some(name) = rc.attribute("name") else { continue };
        let (tag, instances) = rcb_instances(rc, name);
        let dat_set = rc.attribute("datSet").filter(|d| !d.is_empty());
        let members = match dat_set.map(|d| (d, datasets.get(d))) {
            Some((_, Some(m))) => m.clone(),
            Some((d, None)) => {
                out.errors.push(BrowseError { reference: format!("{ln_ref}.{tag}.{name}"), error: format!("DataSet {d:?} tidak ada") });
                Vec::new()
            }
            None => Vec::new(),
        };
        let trg = children(rc, "TrgOps").next();
        // period = integrity; gi default true (skema SCL) kalau TrgOps ada
        let trg_ops = [("dchg", "dchg", false), ("qchg", "qchg", false), ("dupd", "dupd", false), ("period", "integrity", false), ("gi", "gi", true)]
            .into_iter()
            .filter(|(k, _, d)| trg.is_some_and(|t| flag(t, k, *d)))
            .map(|(_, n, _)| n)
            .collect::<Vec<_>>()
            .join(",");
        let opt_flds = children(rc, "OptFields").next().map(|o| {
            // nama & urutan sama dengan BROWSE (mapper::opt_flds_text)
            OPT_FLDS.iter().map(|(_, k)| *k).filter(|k| flag(o, k, false)).collect::<Vec<_>>().join(",")
        });
        for inst in instances {
            out.rcbs.push(RcbInfo {
                reference: format!("{ln_ref}.{tag}.{inst}"),
                buffered: tag == "BR",
                dataset: dat_set.map(|d| format!("{ln_ref}.{d}")),
                rpt_id: rc.attribute("rptID").filter(|r| !r.is_empty()).map(String::from),
                rpt_ena: None, // tidak diketahui dari SCL
                trg_ops: trg_ops.clone(),
                intg_pd_ms: num(rc, "intgPd").unwrap_or(0),
                conf_rev: num(rc, "confRev"),
                buf_tm_ms: num(rc, "bufTime"),
                opt_flds: opt_flds.clone(),
                read_error: None,
                members: members.clone(),
            });
        }
    }
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

    // ------------------------------------------------------------ katalog (SCL_IMPORT)

    const CID: &str = include_str!("../tests/fixtures/scl/baseline_1ied.cid");
    const SCD: &str = include_str!("../tests/fixtures/scl/station_2ied.scd");

    fn ctl(model: &str, sbo: Option<i64>, sbow: bool, cancel: bool) -> Option<ControlInfo> {
        Some(ControlInfo { ctl_model: Some(model.into()), sbo_timeout_ms: sbo, sbow, oper: true, cancel })
    }

    fn bo(r: &str, fc: &str, cdc: &str, control: Option<ControlInfo>) -> BrowseObject {
        BrowseObject { object_reference: r.into(), fc: fc.into(), cdc: Some(cdc.into()), control }
    }

    fn member(r: &str, fc: &str) -> DatasetMember {
        DatasetMember { object_reference: r.into(), fc: fc.into() }
    }

    /// (a) CID 1 IED: objek (urut DO, FC, SDO), ctlModel instance menimpa default, dataset, RCB, config_revs, ip.
    #[test]
    fn katalog_cid_satu_ied() {
        let c = catalog(CID, None).unwrap();
        assert_eq!(
            c.ied,
            IedInfo {
                name: "Baseline".into(),
                manufacturer: Some("Schneider Electric".into()),
                ied_type: Some("Baseline".into()),
                config_version: Some("1.0".into()),
                ip: Some("10.10.1.21".into()),
            }
        );
        let ld = "BaselineLD0";
        assert_eq!(
            c.result.objects,
            vec![
                bo(&format!("{ld}/LLN0.Mod"), "ST", "ENC", None),
                // DOI status-only menimpa default direct-with-normal-security
                bo(&format!("{ld}/LLN0.Mod"), "CO", "ENC", ctl("status-only", None, false, false)),
                bo(&format!("{ld}/XSWI1.OpCnt"), "ST", "INS", None),
                bo(&format!("{ld}/XSWI1.Pos"), "ST", "DPC", None),
                bo(&format!("{ld}/XSWI1.Pos"), "CO", "DPC", ctl("sbo-with-enhanced-security", Some(30000), true, true)),
                bo(&format!("{ld}/MainCSWI1.Pos"), "ST", "DPC", None),
                // tanpa DOI: default DOType
                bo(&format!("{ld}/MainCSWI1.Pos"), "CO", "DPC", ctl("direct-with-normal-security", None, true, true)),
                bo(&format!("{ld}/MMXU1.PhV.phsA"), "MX", "CMV", None),
                bo(&format!("{ld}/MMXU1.PhV.phsB"), "MX", "CMV", None),
            ]
        );
        let refs: Vec<&str> = c.result.rcbs.iter().map(|r| r.reference.as_str()).collect();
        assert_eq!(refs, vec![format!("{ld}/LLN0.BR.brcbST01"), format!("{ld}/LLN0.BR.brcbST02"), format!("{ld}/LLN0.RP.urcbMX01")]);
        let st = &c.result.rcbs[0];
        assert_eq!(
            (st.buffered, st.dataset.as_deref(), st.rpt_id.as_deref(), st.rpt_ena, st.trg_ops.as_str(), st.intg_pd_ms),
            (true, Some("BaselineLD0/LLN0.dsStatus"), None, None, "dchg,qchg,gi", 0)
        );
        assert_eq!((st.conf_rev, st.buf_tm_ms), (Some(1), Some(100)));
        assert_eq!(st.opt_flds.as_deref(), Some("seqNum,timeStamp,dataSet,reasonCode,entryID,configRef"));
        assert_eq!(st.members, vec![member(&format!("{ld}/XSWI1.Pos.stVal"), "ST"), member(&format!("{ld}/XSWI1.Pos.q"), "ST")]);
        assert_eq!(c.result.rcbs[1].members, st.members);
        let mx = &c.result.rcbs[2];
        assert_eq!(
            (mx.buffered, mx.rpt_id.as_deref(), mx.trg_ops.as_str(), mx.intg_pd_ms, mx.conf_rev, mx.buf_tm_ms, mx.opt_flds.as_deref()),
            (false, Some("MXReport"), "dchg,integrity", 5000, Some(3), None, Some("timeStamp"))
        );
        assert_eq!(mx.members, vec![member(&format!("{ld}/MMXU1.PhV.phsA"), "MX")]);
        assert_eq!(
            c.config_revs,
            vec![ConfigRev { ld: ld.into(), reference: format!("{ld}/LLN0.NamPlt.configRev"), value: "Rev 2026-11".into() }]
        );
        assert!(c.result.errors.is_empty(), "{:?}", c.result.errors);
    }

    /// (b) SCD 2 IED: tanpa ied_name -> Pilih IED + ied_names; dengan ied_name -> IED itu (ldName dipakai).
    /// (c) ied_name salah -> gagal + ied_names.
    #[test]
    fn katalog_scd_pilih_ied() {
        let names = Some(vec![IedRef { name: "IED1".into(), ip: Some("10.0.0.1".into()) }, IedRef { name: "IED2".into(), ip: Some("10.0.0.2".into()) }]);
        assert_eq!(catalog(SCD, None).unwrap_err(), SclError { message: "Pilih IED".into(), ied_names: names.clone() });
        let c = catalog(SCD, Some("IED2")).unwrap();
        assert_eq!((c.ied.name.as_str(), c.ied.ip.as_deref(), c.ied.config_version.as_deref()), ("IED2", Some("10.0.0.2"), Some("2")));
        assert_eq!(c.result.objects, vec![bo("BAY2_CTRL/GGIO2.Ind1", "ST", "SPS", None)]);
        assert!(c.config_revs.is_empty());
        assert_eq!(catalog(SCD, Some("IED1")).unwrap().result.objects[0].object_reference, "IED1LD0/GGIO1.Ind1");
        assert_eq!(catalog(SCD, Some("IEDX")).unwrap_err(), SclError { message: "IED IEDX tidak ada di file".into(), ied_names: names });
    }

    /// (d) XML rusak / bukan SCL / DTD (entity eksternal) -> gagal.
    #[test]
    fn katalog_bukan_scl() {
        for xml in [
            "<SCL><IED name=\"x\">",
            "<foo/>",
            "",
            r#"<?xml version="1.0"?><!DOCTYPE SCL [<!ENTITY x SYSTEM "file:///etc/passwd">]><SCL>&x;</SCL>"#,
        ] {
            let e = catalog(xml, None).unwrap_err();
            assert!(e.message.starts_with("File bukan SCL yang valid"), "{xml}: {e}");
            assert!(e.ied_names.is_none());
        }
        assert_eq!(catalog("<SCL/>", None).unwrap_err().message, "File SCL tidak berisi IED");
    }

    /// (e) tipe DO hilang -> objek dilewati + errors, tetap sukses.
    #[test]
    fn katalog_tipe_hilang_masuk_errors() {
        let xml = CID.replace(r#"<DO name="OpCnt" type="INS_T"/>"#, r#"<DO name="OpCnt" type="TIDAK_ADA"/>"#);
        assert_ne!(xml, CID);
        let c = catalog(&xml, None).unwrap();
        assert_eq!(
            c.result.errors,
            vec![BrowseError { reference: "BaselineLD0/XSWI1.OpCnt".into(), error: "DOType \"TIDAK_ADA\" tidak ada di DataTypeTemplates".into() }]
        );
        assert_eq!(c.result.objects.len(), 8);
        assert!(!c.result.objects.iter().any(|o| o.object_reference.ends_with("OpCnt")));
    }

    /// (f) format sama dengan keluaran BROWSE (contoh IEC61850_BROWSE_RESPONSE 9302 Schneider Baseline di docs/examples).
    #[test]
    fn katalog_sama_dengan_contoh_browse() {
        let browse = crate::kafka::payload_examples::example("produce", "IEC61850_BROWSE_RESPONSE__success_with_errors");
        let objects: Vec<BrowseObject> = serde_json::from_value(browse["objects"].clone()).unwrap();
        let rcbs: Vec<RcbInfo> = serde_json::from_value(browse["rcbs"].clone()).unwrap();
        let c = catalog(CID, None).unwrap();
        // objek XSWI1 (termasuk Pos CO + control) identik
        let xswi: Vec<_> = c.result.objects.iter().filter(|o| o.object_reference.starts_with("BaselineLD0/XSWI1.")).cloned().collect();
        assert_eq!(xswi, objects);
        // RCB brcbST01: reference, dataset, trg_ops, members identik (rpt_id / rpt_ena hanya diketahui dari IED)
        let got = &c.result.rcbs[0];
        let want = &rcbs[0];
        assert_eq!(
            (&got.reference, got.buffered, &got.dataset, &got.trg_ops, got.intg_pd_ms, &got.members),
            (&want.reference, want.buffered, &want.dataset, &want.trg_ops, want.intg_pd_ms, &want.members)
        );
        // conf_rev / buf_tm_ms / opt_flds: BROWSE (dari IED) dan SCL berformat sama
        assert_eq!((got.conf_rev, got.buf_tm_ms, &got.opt_flds), (want.conf_rev, want.buf_tm_ms, &want.opt_flds));
    }

    /// ReportControl di LN biasa (bukan LLN0), seperti `MainCSWI1.RP.brcbName01` IED 9302: ikut ke katalog & parse.
    #[test]
    fn rcb_di_ln_selain_lln0() {
        let ln = r#"<LN lnClass="CSWI" inst="1" prefix="Main" lnType="CSWI_T">
            <DataSet name="dsCtl"><FCDA ldInst="LD0" prefix="Main" lnClass="CSWI" lnInst="1" doName="Pos" fc="ST"/></DataSet>
            <ReportControl name="brcbName" datSet="dsCtl" confRev="1" buffered="false"><TrgOps dchg="true"/></ReportControl>
          </LN>"#;
        let xml = CID.replace(r#"<LN lnClass="CSWI" inst="1" prefix="Main" lnType="CSWI_T"/>"#, ln);
        assert_ne!(xml, CID);
        let c = catalog(&xml, None).unwrap();
        let r = c.result.rcbs.iter().find(|r| r.reference == "BaselineLD0/MainCSWI1.RP.brcbName01").expect("RCB di MainCSWI1");
        assert_eq!((r.buffered, r.dataset.as_deref(), r.trg_ops.as_str()), (false, Some("BaselineLD0/MainCSWI1.dsCtl"), "dchg"));
        assert_eq!(r.members, vec![member("BaselineLD0/MainCSWI1.Pos", "ST")]);
        assert!(c.result.errors.is_empty(), "{:?}", c.result.errors);
        assert!(parse(&xml, None).unwrap().rcbs.contains(&"BaselineLD0/MainCSWI1.RP.brcbName01".to_string()));
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
