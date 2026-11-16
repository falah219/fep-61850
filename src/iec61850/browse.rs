//! Browse model IED (event BROWSE): LD -> LN -> DO -> FC, CDC tebakan dari struktur DO, info kontrol & RCB.
//!
//! Per LN: satu `getVariableSpecification` per FC (cepat). Kalau gagal (mis. `malformed-message`: respons type
//! spec LN terlalu besar untuk PDU / tidak bisa di-parse libIEC61850), struktur dibangun dari GetNameList
//! (`getLogicalDeviceVariables`, nama MMS `LN$FC$DO$DA`); tipe `stVal` dari read nilai (BROWSE_READ_VALUES).
//! Sebagian IED (mis. Schneider Baseline) membalas `malformed-message` untuk LN+FC yang memang tidak ada:
//! kalau GetNameList juga tidak punya variabel FC itu, FC dianggap tidak ada (hanya log DEBUG, bukan error).
//! Kegagalan nyata satu node dicatat di `errors` dan browse lanjut ke node berikutnya.
//!
//! Dikerjakan bertahap (`BrowseJob::step`, satu LN per langkah) di thread IED, diselingi polling & command,
//! dibatasi BROWSE_TIMEOUT_MS. CDC dari tipe MMS: ENS/ENC tidak bisa dibedakan dari INS/INC -> INS / INC.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::debug;

use super::client::{cstring, fc_from_str, Ied, Rcb, SpecNode, SpecType};
use super::mapper::trg_ops_text;
use crate::error::{Iec61850Error, Result};
use crate::ffi;

/// FC yang bisa dimapping (CHECK `functional_constraint` di `master_datapoint_iec61850s`).
const FCS: [&str; 4] = ["ST", "MX", "CO", "SP"];
/// Kedalaman sub-DO yang dicari kalau CDC DO tidak dikenali (mis. WYE.phsA -> CMV).
const MAX_DEPTH: usize = 3;
/// Default BROWSE_TIMEOUT_MS (batas total satu browse).
pub const DEFAULT_BROWSE_TIMEOUT_MS: u64 = 300_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrowseObject {
    pub object_reference: String,
    pub fc: String,
    pub cdc: Option<String>,
    /// Hanya objek FC `CO`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ControlInfo>,
}

/// Info kontrol satu DO (FC CO).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ControlInfo {
    /// Teks sama dengan kolom `ctl_model` (`sbo-with-enhanced-security`, ...); null = tidak dibaca / gagal
    pub ctl_model: Option<String>,
    /// CF `sboTimeout` (ms); null = tidak ada / tidak dibaca
    pub sbo_timeout_ms: Option<i64>,
    pub sbow: bool,
    pub oper: bool,
    pub cancel: bool,
}

/// Node yang gagal dibrowse (browse tetap lanjut).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrowseError {
    #[serde(rename = "ref")]
    pub reference: String,
    pub error: String,
}

/// RCB (BRCB `.BR.` / URCB `.RP.`) untuk setup mode REPORT. Urutan field = urutan JSON.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RcbInfo {
    pub reference: String,
    pub buffered: bool,
    /// Format ACSI `LD/LLN0.DataSet`
    pub dataset: Option<String>,
    pub rpt_id: Option<String>,
    pub rpt_ena: bool,
    /// `dchg,qchg,dupd,integrity,gi` (format kolom trg_ops)
    pub trg_ops: String,
    pub intg_pd_ms: u32,
    pub members: Vec<DatasetMember>,
}

/// Anggota dataset, bentuk sama dengan `objects[]`: `LD/LN.DO.DA` + FC.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatasetMember {
    pub object_reference: String,
    /// `""` kalau IED tidak menyertakan FC
    pub fc: String,
}

/// `LD/XSWI1.Pos.stVal[ST]` (format GetDataSetDirectory libIEC61850) -> {object_reference, fc}.
pub fn parse_member(s: &str) -> DatasetMember {
    let s = s.trim();
    match s.strip_suffix(']').and_then(|x| x.rsplit_once('[')) {
        Some((r, fc)) => DatasetMember { object_reference: r.into(), fc: fc.into() },
        None => DatasetMember { object_reference: s.into(), fc: String::new() },
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct BrowseResult {
    pub objects: Vec<BrowseObject>,
    pub errors: Vec<BrowseError>,
    pub rcbs: Vec<RcbInfo>,
}

#[derive(Debug, Clone, Copy)]
pub struct BrowseOptions {
    /// BROWSE_READ_VALUES (default true): baca tipe stVal (fallback), ctlModel & sboTimeout
    pub read_values: bool,
    pub timeout: Duration,
}

impl BrowseOptions {
    pub fn from_env() -> Self {
        let read_values = crate::logger::env_flag(std::env::var("BROWSE_READ_VALUES").ok().as_deref(), true);
        Self { read_values, timeout: browse_timeout() }
    }
}

/// Env BROWSE_TIMEOUT_MS (> 0), default 300000.
pub fn browse_timeout() -> Duration {
    let ms = std::env::var("BROWSE_TIMEOUT_MS").ok().and_then(|v| v.trim().parse::<u64>().ok()).filter(|v| *v > 0);
    Duration::from_millis(ms.unwrap_or(DEFAULT_BROWSE_TIMEOUT_MS))
}

/// ctlModel (CF) -> teks kolom `ctl_model`.
pub fn ctl_model_name(v: i64) -> String {
    match v {
        0 => "status-only",
        1 => "direct-with-normal-security",
        2 => "sbo-with-normal-security",
        3 => "direct-with-enhanced-security",
        4 => "sbo-with-enhanced-security",
        _ => return v.to_string(),
    }
    .into()
}

/// IED menjawab "tidak ada" / spec kosong: node ini memang tidak punya FC / atribut tsb.
fn is_absent(e: &Iec61850Error) -> bool {
    e.is_server_response() || e.code().is_none()
}

/// Browse bertahap. `start` (daftar LD/LN) lalu `step` berulang sampai true, lalu `finish`.
pub struct BrowseJob {
    pending: VecDeque<(String, String)>,
    /// Cache GetNameList per LD (fallback); Err = gagal, sudah dicatat
    ld_names: HashMap<String, std::result::Result<Vec<String>, String>>,
    /// Cache anggota dataset
    datasets: HashMap<String, Vec<DatasetMember>>,
    opts: BrowseOptions,
    deadline: Instant,
    result: BrowseResult,
}

impl BrowseJob {
    /// Daftar LD & LN (GetNameList). Err hanya kalau daftar LD tidak terbaca / koneksi putus.
    pub fn start(ied: &Ied, opts: BrowseOptions) -> Result<Self> {
        let mut job = Self {
            pending: VecDeque::new(),
            ld_names: HashMap::new(),
            datasets: HashMap::new(),
            opts,
            deadline: Instant::now() + opts.timeout,
            result: BrowseResult::default(),
        };
        for ld in ied.logical_devices()? {
            match ied.logical_nodes(&ld) {
                Ok(lns) => job.pending.extend(lns.into_iter().map(|ln| (ld.clone(), ln))),
                Err(e) if e.is_connection_lost() => return Err(e),
                Err(e) => job.error(&ld, &e),
            }
        }
        Ok(job)
    }

    /// Satu LN. Ok(true) = selesai (semua LN atau BROWSE_TIMEOUT_MS). Err = koneksi putus.
    pub fn step(&mut self, ied: &Ied) -> Result<bool> {
        if Instant::now() >= self.deadline && !self.pending.is_empty() {
            let left = self.pending.len();
            let ms = self.opts.timeout.as_millis();
            self.error("browse", &format!("BROWSE_TIMEOUT_MS ({ms} ms) tercapai, {left} LN belum dibrowse"));
            self.pending.clear();
        }
        let Some((ld, ln)) = self.pending.pop_front() else { return Ok(true) };
        self.browse_ln(ied, &ld, &ln)?;
        Ok(self.pending.is_empty())
    }

    pub fn finish(self) -> BrowseResult {
        self.result
    }

    fn error(&mut self, reference: &str, e: &dyn std::fmt::Display) {
        self.result.errors.push(BrowseError { reference: reference.into(), error: e.to_string() });
    }

    fn browse_ln(&mut self, ied: &Ied, ld: &str, ln: &str) -> Result<()> {
        let ln_ref = format!("{ld}/{ln}");
        let mut specs = Vec::new();
        let mut failed: Vec<(&'static str, String)> = Vec::new();
        for fc in FCS {
            match ied.variable_spec(&ln_ref, fc_from_str(fc)?) {
                Ok(s) => specs.push((fc, s.tree())),
                Err(e) if is_absent(&e) => {}
                Err(e) if e.is_connection_lost() => return Err(e),
                Err(e) => failed.push((fc, e.to_string())),
            }
        }
        if !failed.is_empty() {
            let names_tree = match self.ld_names(ied, ld)? {
                Ok(names) => Ok(tree_from_names(ln, names)),
                Err(e) => Err(e),
            };
            specs.extend(fallback_specs(&ln_ref, &failed, names_tree, &mut self.result.errors));
            specs.sort_by_key(|(f, _)| FCS.iter().position(|x| x == f));
            // tipe stVal hanya perlu dibaca kalau struktur ST dari GetNameList (tanpa tipe)
            if self.opts.read_values && failed.iter().any(|(f, _)| *f == "ST") {
                if let Some((_, st)) = specs.iter_mut().find(|(f, _)| *f == "ST") {
                    fill_stval_types(ied, &ln_ref, st, 0, &mut self.result.errors)?;
                }
            }
        }
        let mut objects = ln_objects(&ln_ref, &specs);
        for o in objects.iter_mut().filter(|o| o.fc == "CO") {
            o.control = Some(control_info(ied, &ln_ref, &specs, &o.object_reference, self.opts.read_values, &mut self.result.errors)?);
        }
        self.result.objects.extend(objects);
        self.rcbs(ied, &ln_ref)
    }

    /// Nama variabel MMS satu LD (GetNameList, sekali per LD). Err dalam = GetNameList gagal.
    fn ld_names(&mut self, ied: &Ied, ld: &str) -> Result<std::result::Result<&[String], String>> {
        if !self.ld_names.contains_key(ld) {
            let names = match ied.logical_device_variables(ld) {
                Ok(v) => Ok(v),
                Err(e) if e.is_connection_lost() => return Err(e),
                Err(e) => Err(e.to_string()),
            };
            self.ld_names.insert(ld.to_string(), names);
        }
        Ok(match self.ld_names.get(ld) {
            Some(Ok(v)) => Ok(v.as_slice()),
            Some(Err(e)) => Err(e.clone()),
            None => Err("GetNameList tidak tersedia".into()),
        })
    }

    fn rcbs(&mut self, ied: &Ied, ln_ref: &str) -> Result<()> {
        for (class, tag) in [(ffi::ACSI_CLASS_BRCB, "BR"), (ffi::ACSI_CLASS_URCB, "RP")] {
            let names = match ied.logical_node_directory(ln_ref, class) {
                Ok(n) => n,
                Err(e) if e.is_connection_lost() => return Err(e),
                Err(e) => {
                    self.error(&format!("{ln_ref} ({tag})"), &e);
                    continue;
                }
            };
            for name in names {
                let reference = format!("{ln_ref}.{tag}.{name}");
                let rcb = match Rcb::read(ied, &reference) {
                    Ok(r) => r,
                    Err(e) if e.is_connection_lost() => return Err(e),
                    Err(e) => {
                        self.error(&reference, &e);
                        continue;
                    }
                };
                let cur = rcb.current();
                let members = match &cur.dataset {
                    Some(ds) => self.members(ied, ds)?,
                    None => Vec::new(),
                };
                self.result.rcbs.push(RcbInfo {
                    reference,
                    buffered: cur.buffered,
                    rpt_id: rcb.rpt_id(),
                    rpt_ena: rcb.rpt_ena(),
                    dataset: cur.dataset,
                    trg_ops: trg_ops_text(cur.trg_ops),
                    intg_pd_ms: cur.intg_pd_ms,
                    members,
                });
            }
        }
        Ok(())
    }

    fn members(&mut self, ied: &Ied, ds: &str) -> Result<Vec<DatasetMember>> {
        if let Some(m) = self.datasets.get(ds) {
            return Ok(m.clone());
        }
        let m = match ied.dataset_directory(ds) {
            Ok(m) => m.iter().map(|s| parse_member(s)).collect(),
            Err(e) if e.is_connection_lost() => return Err(e),
            Err(e) => {
                self.error(ds, &e);
                Vec::new()
            }
        };
        self.datasets.insert(ds.to_string(), m.clone());
        Ok(m)
    }
}

/// FC yang getVariableSpecification-nya gagal: strukturnya diambil dari GetNameList (`names_tree`).
/// - GetNameList tidak punya variabel FC itu -> FC memang tidak ada (IED membalas malformed alih-alih
///   object-non-existent): BUKAN error, hanya DEBUG.
/// - GetNameList gagal -> error per FC (fallback juga gagal).
pub fn fallback_specs(
    ln_ref: &str,
    failed: &[(&'static str, String)],
    names_tree: std::result::Result<Vec<(&'static str, SpecNode)>, String>,
    errors: &mut Vec<BrowseError>,
) -> Vec<(&'static str, SpecNode)> {
    match names_tree {
        Err(e) => {
            for (fc, spec_err) in failed {
                errors.push(BrowseError {
                    reference: format!("{ln_ref}[{fc}]"),
                    error: format!("getVariableSpecification: {spec_err}; GetNameList: {e}"),
                });
            }
            Vec::new()
        }
        Ok(tree) => failed
            .iter()
            .filter_map(|(fc, spec_err)| {
                let found = tree.iter().find(|(f, _)| f == fc).cloned();
                if found.is_none() {
                    debug!("{ln_ref}[{fc}]: getVariableSpecification {spec_err}, GetNameList tanpa variabel FC ini -> FC tidak ada");
                }
                found
            })
            .collect(),
    }
}

/// Fallback GetNameList: nama MMS LD (`LN$FC$DO$DA...`) -> struktur per FC untuk LN `ln` (urutan FCS).
/// Tipe leaf `Other` (kecuali diisi `fill_stval_types`).
pub fn tree_from_names(ln: &str, names: &[String]) -> Vec<(&'static str, SpecNode)> {
    let mut roots: Vec<(&'static str, SpecNode)> = Vec::new();
    for name in names {
        let mut parts = name.split('$');
        if parts.next() != Some(ln) {
            continue;
        }
        let Some(fc) = parts.next().and_then(|f| FCS.iter().copied().find(|x| *x == f)) else { continue };
        let path: Vec<&str> = parts.collect();
        if path.is_empty() {
            continue;
        }
        let i = match roots.iter().position(|(f, _)| *f == fc) {
            Some(i) => i,
            None => {
                roots.push((fc, SpecNode { name: ln.into(), ty: SpecType::Struct, children: Vec::new() }));
                roots.len() - 1
            }
        };
        insert_path(&mut roots[i].1, &path);
    }
    roots.sort_by_key(|(f, _)| FCS.iter().position(|x| x == f));
    roots
}

fn insert_path(node: &mut SpecNode, path: &[&str]) {
    let Some((first, rest)) = path.split_first() else { return };
    node.ty = SpecType::Struct;
    let i = match node.children.iter().position(|c| c.name == *first) {
        Some(i) => i,
        None => {
            node.children.push(SpecNode { name: (*first).into(), ty: SpecType::Other, children: Vec::new() });
            node.children.len() - 1
        }
    };
    insert_path(&mut node.children[i], rest);
}

/// Isi tipe `stVal` (ST) dari read nilai, untuk DO dan sub-DO (maks MAX_DEPTH).
fn fill_stval_types(ied: &Ied, prefix: &str, node: &mut SpecNode, depth: usize, errors: &mut Vec<BrowseError>) -> Result<()> {
    for d in node.children.iter_mut() {
        let r = format!("{prefix}.{}", d.name);
        if let Some(sv) = d.children.iter_mut().find(|c| c.name == "stVal") {
            match ied.read_object(&cstring(&format!("{r}.stVal"))?, ffi::IEC61850_FC_ST) {
                Ok(v) => sv.ty = v.spec_type(),
                Err(e) if e.is_connection_lost() => return Err(e),
                Err(e) => errors.push(BrowseError { reference: format!("{r}.stVal[ST]"), error: e.to_string() }),
            }
        } else if depth < MAX_DEPTH && d.ty == SpecType::Struct {
            fill_stval_types(ied, &r, d, depth + 1, errors)?;
        }
    }
    Ok(())
}

fn node_at<'a>(n: &'a SpecNode, path: &[&str]) -> Option<&'a SpecNode> {
    path.iter().try_fold(n, |n, p| n.children.iter().find(|c| c.name == *p))
}

/// SBOw/Oper/Cancel dari struktur CO; ctlModel & sboTimeout (CF) dari read nilai.
fn control_info(
    ied: &Ied,
    ln_ref: &str,
    specs: &[(&str, SpecNode)],
    obj_ref: &str,
    read_values: bool,
    errors: &mut Vec<BrowseError>,
) -> Result<ControlInfo> {
    let mut info = control_flags(ln_ref, specs, obj_ref);
    if read_values {
        info.ctl_model = read_cf_int(ied, obj_ref, "ctlModel", true, errors)?.map(ctl_model_name);
        info.sbo_timeout_ms = read_cf_int(ied, obj_ref, "sboTimeout", false, errors)?;
    }
    Ok(info)
}

/// SBOw / Oper / Cancel yang ada di struktur CO objek ini.
fn control_flags(ln_ref: &str, specs: &[(&str, SpecNode)], obj_ref: &str) -> ControlInfo {
    let path: Vec<&str> = obj_ref.get(ln_ref.len() + 1..).unwrap_or_default().split('.').collect();
    let co = specs.iter().find(|(f, _)| *f == "CO").and_then(|(_, n)| node_at(n, &path));
    let has = |name: &str| co.is_some_and(|c| c.children.iter().any(|x| x.name == name));
    ControlInfo { sbow: has("SBOw"), oper: has("Oper"), cancel: has("Cancel"), ..Default::default() }
}

/// Baca atribut CF integer. `required` = gagal dicatat walau IED menjawab "tidak ada".
fn read_cf_int(ied: &Ied, obj_ref: &str, attr: &str, required: bool, errors: &mut Vec<BrowseError>) -> Result<Option<i64>> {
    match ied.read_object(&cstring(&format!("{obj_ref}.{attr}"))?, ffi::IEC61850_FC_CF) {
        Ok(v) => Ok(v.as_i64()),
        Err(e) if e.is_connection_lost() => Err(e),
        Err(e) => {
            if required || !is_absent(&e) {
                errors.push(BrowseError { reference: format!("{obj_ref}.{attr}[CF]"), error: e.to_string() });
            }
            Ok(None)
        }
    }
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
        .map(|(fc, _)| BrowseObject { object_reference: r.to_string(), fc: fc.to_string(), cdc: cdc.map(String::from), control: None })
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
        BrowseObject { object_reference: r.into(), fc: fc.into(), cdc: cdc.map(String::from), control: None }
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

    /// Nama GetNameList contoh (bentuk Schneider Baseline Ed2): termasuk node antara (`XSWI1$ST`),
    /// FC yang tidak dimapping (CF/DC), dan LN lain berawalan sama (`GGIO10` vs `GGIO1`).
    fn names() -> Vec<String> {
        [
            "XSWI1", "XSWI1$ST", "XSWI1$ST$Pos", "XSWI1$ST$Pos$stVal", "XSWI1$ST$Pos$q", "XSWI1$ST$Pos$t",
            "XSWI1$CO", "XSWI1$CO$Pos", "XSWI1$CO$Pos$SBOw", "XSWI1$CO$Pos$SBOw$ctlVal", "XSWI1$CO$Pos$Oper",
            "XSWI1$CO$Pos$Oper$ctlVal", "XSWI1$CO$Pos$Cancel", "XSWI1$CF$Pos$ctlModel", "XSWI1$CF$Pos$sboTimeout",
            "XSWI1$DC$Pos$d", "XSWI1$ST$OpCnt$stVal", "XSWI1$MX$A$phsA$cVal$mag$f", "XSWI1$MX$A$phsB$cVal$mag$f",
            "GGIO10$ST$Ind1$stVal", "GGIO1$ST$Ind1$stVal",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn set_type(specs: &mut [(&str, SpecNode)], fc: &str, path: &[&str], ty: SpecType) {
        let root = &mut specs.iter_mut().find(|(f, _)| *f == fc).unwrap().1;
        let node = path.iter().fold(root, |n, p| n.children.iter_mut().find(|c| c.name == *p).unwrap());
        node.ty = ty;
    }

    #[test]
    fn fallback_getnamelist_ke_struktur() {
        let mut specs = tree_from_names("XSWI1", &names());
        // hanya FC yang dimapping, urutan FCS; CF/DC dan LN lain dibuang
        assert_eq!(specs.iter().map(|(f, _)| *f).collect::<Vec<_>>(), vec!["ST", "MX", "CO"]);
        let st = &specs[0].1;
        assert_eq!(st.children.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["Pos", "OpCnt"]);
        assert_eq!(st.children[0].children.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["stVal", "q", "t"]);
        let r = "LD/XSWI1";
        // tanpa read nilai (BROWSE_READ_VALUES=false): tipe stVal tidak diketahui -> cdc null
        let objs = ln_objects(r, &specs);
        assert!(objs.iter().any(|o| o.object_reference == "LD/XSWI1.Pos" && o.fc == "CO" && o.cdc.is_none()));
        // dengan tipe dari read nilai: Pos BIT STRING + Oper -> DPC, OpCnt integer -> INS, A.phsA -> CMV
        set_type(&mut specs, "ST", &["Pos", "stVal"], SpecType::BitString);
        set_type(&mut specs, "ST", &["OpCnt", "stVal"], SpecType::Int);
        assert_eq!(
            ln_objects(r, &specs),
            vec![
                obj("LD/XSWI1.A.phsA", "MX", Some("CMV")),
                obj("LD/XSWI1.A.phsB", "MX", Some("CMV")),
                obj("LD/XSWI1.OpCnt", "ST", Some("INS")),
                obj("LD/XSWI1.Pos", "ST", Some("DPC")),
                obj("LD/XSWI1.Pos", "CO", Some("DPC")),
            ]
        );
        assert_eq!(
            control_flags(r, &specs, "LD/XSWI1.Pos"),
            ControlInfo { sbow: true, oper: true, cancel: true, ..Default::default() }
        );
        assert_eq!(control_flags(r, &specs, "LD/XSWI1.OpCnt"), ControlInfo::default());
        // LN berawalan sama tidak tercampur
        let g = tree_from_names("GGIO1", &names());
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].1.children.len(), 1);
        assert!(tree_from_names("TIDAKADA", &names()).is_empty());
    }

    /// (a) malformed + GetNameList tanpa variabel FC itu -> FC tidak ada, TIDAK masuk errors.
    #[test]
    fn malformed_dan_getnamelist_kosong_bukan_error() {
        let failed = vec![("MX", "malformed-message".to_string()), ("CO", "malformed-message".to_string())];
        let mut errors = Vec::new();
        // GetNameList LN ini hanya punya ST
        let names = tree_from_names("MainXSWI1", &["MainXSWI1$ST$Pos$stVal".to_string()]);
        assert!(fallback_specs("LD/MainXSWI1", &failed, Ok(names), &mut errors).is_empty());
        assert!(errors.is_empty(), "{errors:?}");
        // LN tanpa variabel sama sekali
        assert!(fallback_specs("LD/LLN0", &failed, Ok(Vec::new()), &mut errors).is_empty());
        assert!(errors.is_empty());
    }

    /// (b) malformed + GetNameList punya variabel FC itu -> struktur dari GetNameList, objek tetap masuk, tanpa error.
    #[test]
    fn malformed_dan_getnamelist_ada_variabel() {
        let failed = vec![("ST", "malformed-message".to_string()), ("MX", "malformed-message".to_string())];
        let mut errors = Vec::new();
        let names_tree = tree_from_names("XSWI1", &names());
        let mut specs = fallback_specs("LD/XSWI1", &failed, Ok(names_tree), &mut errors);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(specs.iter().map(|(f, _)| *f).collect::<Vec<_>>(), vec!["ST", "MX"]);
        set_type(&mut specs, "ST", &["Pos", "stVal"], SpecType::BitString);
        let objs = ln_objects("LD/XSWI1", &specs);
        assert!(objs.contains(&obj("LD/XSWI1.Pos", "ST", Some("DPS"))), "{objs:?}");
        assert!(objs.contains(&obj("LD/XSWI1.A.phsA", "MX", Some("CMV"))));
    }

    /// GetNameList juga gagal -> baru masuk errors (per FC), tanpa teks internal.
    #[test]
    fn getnamelist_gagal_masuk_errors() {
        let failed = vec![("ST", "malformed-message".to_string())];
        let mut errors = Vec::new();
        assert!(fallback_specs("LD/X", &failed, Err("timeout".into()), &mut errors).is_empty());
        assert_eq!(
            errors,
            vec![BrowseError { reference: "LD/X[ST]".into(), error: "getVariableSpecification: malformed-message; GetNameList: timeout".into() }]
        );
        assert!(!errors[0].error.contains("pakai"));
    }

    /// (c) anggota dataset "LD/LN.DO[FC]" -> {object_reference, fc}.
    #[test]
    fn parse_anggota_dataset() {
        let m = |r: &str, fc: &str| DatasetMember { object_reference: r.into(), fc: fc.into() };
        assert_eq!(parse_member("BaselineLD0/XSWI1.Pos.stVal[ST]"), m("BaselineLD0/XSWI1.Pos.stVal", "ST"));
        assert_eq!(parse_member("LD/MMXU1.A[MX]"), m("LD/MMXU1.A", "MX"));
        assert_eq!(parse_member(" LD/GGIO1.Ind1 "), m("LD/GGIO1.Ind1", ""));
    }

    #[test]
    fn nama_ctl_model() {
        assert_eq!(ctl_model_name(4), "sbo-with-enhanced-security");
        assert_eq!(ctl_model_name(1), "direct-with-normal-security");
        assert_eq!(ctl_model_name(9), "9");
    }
}
