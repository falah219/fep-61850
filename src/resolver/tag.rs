use dashmap::{DashMap, DashSet};
use std::sync::Arc;

pub struct TagResolver {
    mapping: Arc<DashMap<(i32, i32), String>>,
    manual_set: Arc<DashSet<String>>,
}

impl TagResolver {
    pub fn new() -> Self {
        Self {
            mapping: Arc::new(DashMap::new()),
            manual_set: Arc::new(DashSet::new()),
        }
    }

    pub fn load(&self, tags: Vec<(i32, i32, String)>) {
        self.mapping.clear();
        for (id_rtu, ioa, tag) in tags {
            self.mapping.insert((id_rtu, ioa), tag);
        }
    }

    pub fn get_tag(&self, id_rtu: i32, ioa: i32) -> Option<String> {
        self.mapping.get(&(id_rtu, ioa)).map(|v| v.clone())
    }

    pub fn add_tag(&self, id_rtu: i32, ioa: i32, tag: String) {
        self.mapping.insert((id_rtu, ioa), tag);
    }

    pub fn remove_tag(&self, id_rtu: i32, ioa: i32) {
        self.mapping.remove(&(id_rtu, ioa));
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.mapping.iter().any(|entry| entry.value() == tag)
    }

    pub fn load_manual_set(&self, tags: Vec<String>) {
        self.manual_set.clear();
        for tag in tags {
            self.manual_set.insert(tag);
        }
    }

    pub fn set_manual_set(&self, tag: &str, manual_set: bool) {
        if manual_set {
            self.manual_set.insert(tag.to_string());
        } else {
            self.manual_set.remove(tag);
        }
    }

    pub fn is_manual_set(&self, tag: &str) -> bool {
        self.manual_set.contains(tag)
    }

    pub fn reload(&self) {
        self.mapping.clear();
    }

    pub fn get_count(&self) -> usize {
        self.mapping.len()
    }

}

impl Default for TagResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TagResolver {
    fn clone(&self) -> Self {
        Self {
            mapping: Arc::clone(&self.mapping),
            manual_set: Arc::clone(&self.manual_set),
        }
    }
}

unsafe impl Send for TagResolver {}
