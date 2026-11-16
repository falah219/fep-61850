//! MASTER_DATA_OBJECT_EVENT: perbarui group per baris `master_data_objects`, tanpa reconnect IED.
//! CREATE/UPDATE berisi baris lengkap (termasuk `protocol`), DELETE hanya berisi `id`.

use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, info, warn};

use super::capture_data_change::reload_groups;
use crate::connection::Manager;
use crate::kafka::event_handler::is_ours;

#[derive(Debug, PartialEq)]
pub(crate) enum GroupPlan {
    Ignore(&'static str),
    Upsert { id: String, initial: String, id_group: String },
    Remove(String),
    /// Field tidak terbaca: muat ulang semua group + WARN
    ReloadAll(&'static str),
}

/// Baris di `data` / `record` / `new_data` kalau ada, selain itu pesan itu sendiri.
fn row(v: &Value) -> &Value {
    ["data", "record", "new_data"].iter().find_map(|k| v.get(*k).filter(|r| r.is_object())).unwrap_or(v)
}

/// `known(id)` = id baris sudah ada di GroupResolver (baris IEC61850).
pub(crate) fn group_plan(v: Option<&Value>, known: impl Fn(&str) -> bool) -> GroupPlan {
    let Some(v) = v.filter(|v| v.is_object()) else { return GroupPlan::ReloadAll("payload bukan objek JSON") };
    let r = row(v);
    let text = |k: &str| r.get(k).or_else(|| v.get(k)).and_then(Value::as_str);
    let event = ["event", "action", "operation"].iter().find_map(|k| v.get(*k).and_then(Value::as_str)).unwrap_or("").to_uppercase();
    let Some(id) = text("id") else { return GroupPlan::ReloadAll("id tidak terbaca") };
    let remove = |why| if known(id) { GroupPlan::Remove(id.into()) } else { GroupPlan::Ignore(why) };
    if matches!(event.as_str(), "DELETE" | "REMOVE") {
        return remove("id bukan group IEC61850");
    }
    match r.get("protocol").and_then(Value::as_str) {
        Some(p) if !is_ours(p) => return GroupPlan::Ignore("protocol bukan IEC61850"),
        Some(_) => {}
        None => return GroupPlan::ReloadAll("protocol tidak terbaca"),
    }
    if r.get("is_deleted").and_then(Value::as_bool) == Some(true) {
        return remove("baris dihapus, belum ada di group");
    }
    match (r.get("initial").and_then(Value::as_str), r.get("id_group").and_then(Value::as_str)) {
        (Some(initial), Some(g)) => GroupPlan::Upsert { id: id.into(), initial: initial.into(), id_group: g.into() },
        // kolom ada tapi null: baris tidak dipakai sebagai group (sama dengan filter load_groups)
        _ if r.get("initial").is_some() && r.get("id_group").is_some() => remove("baris tanpa initial / id_group"),
        _ => GroupPlan::ReloadAll("initial / id_group tidak terbaca"),
    }
}

pub(crate) async fn master_data_object(mgr: &Arc<Manager>, v: Option<Value>) {
    let groups = &mgr.publisher.groups;
    match group_plan(v.as_ref(), |id| groups.has_row(id)) {
        GroupPlan::Ignore(why) => debug!("[MASTER_DATA_OBJECT] diabaikan: {why}"),
        GroupPlan::Upsert { id, initial, id_group } => {
            info!("[MASTER_DATA_OBJECT] group {initial} = {id_group} (id {id})");
            groups.upsert_row(&id, initial, id_group);
        }
        GroupPlan::Remove(id) => {
            info!("[MASTER_DATA_OBJECT] group id {id} dihapus");
            groups.remove_row(&id);
        }
        GroupPlan::ReloadAll(why) => {
            warn!("[MASTER_DATA_OBJECT] {why}, muat ulang semua group");
            reload_groups(mgr).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::payload_examples::example;
    use crate::resolver::GroupResolver;
    use serde_json::json;

    fn ex(name: &str) -> Value {
        example("consume", &format!("MASTER_DATA_OBJECT_EVENT__{name}"))
    }

    fn apply(g: &GroupResolver, v: &Value) -> GroupPlan {
        let p = group_plan(Some(v), |id| g.has_row(id));
        match &p {
            GroupPlan::Upsert { id, initial, id_group } => g.upsert_row(id, initial.clone(), id_group.clone()),
            GroupPlan::Remove(id) => assert!(g.remove_row(id)),
            _ => {}
        }
        p
    }

    #[test]
    fn create_update_iec104_diabaikan() {
        let g = GroupResolver::new();
        assert_eq!(apply(&g, &ex("UPDATE.iec104")), GroupPlan::Ignore("protocol bukan IEC61850"));
        let mut create = ex("UPDATE.iec104");
        create["event"] = "CREATE".into();
        assert!(matches!(apply(&g, &create), GroupPlan::Ignore(_)));
        assert_eq!(g.get_count(), 0);
    }

    #[test]
    fn create_update_is_deleted_iec61850() {
        let g = GroupResolver::new();
        let create = ex("CREATE");
        let id = create["data"]["id"].as_str().unwrap().to_string();
        assert!(matches!(apply(&g, &create), GroupPlan::Upsert { .. }));
        assert_eq!(g.get_group("MV").as_deref(), create["data"]["id_group"].as_str());
        // update id_group
        let mut update = create.clone();
        update["event"] = "UPDATE".into();
        update["data"]["id_group"] = "11111111-2222-4333-8444-555555555555".into();
        apply(&g, &update);
        assert_eq!(g.get_group("MV").as_deref(), Some("11111111-2222-4333-8444-555555555555"));
        // is_deleted
        update["data"]["is_deleted"] = true.into();
        assert_eq!(apply(&g, &update), GroupPlan::Remove(id));
        assert_eq!(g.get_group("MV"), None);
    }

    #[test]
    fn update_mengganti_initial() {
        let g = GroupResolver::new();
        let mut v = ex("CREATE");
        apply(&g, &v);
        v["event"] = "UPDATE".into();
        v["data"]["initial"] = "CMV".into();
        apply(&g, &v);
        assert_eq!(g.get_group("MV"), None);
        assert!(g.get_group("CMV").is_some());
        assert_eq!(g.get_count(), 1);
    }

    #[test]
    fn delete_hanya_id() {
        let g = GroupResolver::new();
        apply(&g, &ex("CREATE"));
        assert!(matches!(apply(&g, &ex("DELETE")), GroupPlan::Remove(_)));
        assert_eq!(g.get_group("MV"), None);
        // id tidak dikenal (milik protokol lain) -> diabaikan
        assert!(matches!(apply(&g, &ex("DELETE")), GroupPlan::Ignore(_)));
    }

    #[test]
    fn tidak_terbaca_muat_ulang_semua() {
        assert!(matches!(group_plan(None, |_| false), GroupPlan::ReloadAll(_)));
        assert!(matches!(group_plan(Some(&json!({"event":"UPDATE"})), |_| false), GroupPlan::ReloadAll(_)));
        let no_protocol = json!({"event":"UPDATE","data":{"id":"x","initial":"MV","id_group":"g"}});
        assert!(matches!(group_plan(Some(&no_protocol), |_| false), GroupPlan::ReloadAll(_)));
    }
}
