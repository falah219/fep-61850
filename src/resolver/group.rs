use dashmap::DashMap;
use std::sync::Arc;

pub struct GroupResolver {
    mapping: Arc<DashMap<String, String>>,
}

impl GroupResolver {
    pub fn new() -> Self {
        Self {
            mapping: Arc::new(DashMap::new()),
        }
    }

    pub fn load(&self, groups: Vec<(String, String)>) {
        self.mapping.clear();
        for (initial, id_group) in groups {
            self.mapping.insert(initial, id_group);
        }
    }

    pub fn get_group(&self, initial: &str) -> Option<String> {
        self.mapping.get(initial).map(|v| v.clone())
    }

    pub fn add_group(&self, initial: String, id_group: String) {
        self.mapping.insert(initial, id_group);
    }

    pub fn remove_group(&self, initial: &str) {
        self.mapping.remove(initial);
    }

    pub fn reload(&self) {
        self.mapping.clear();
    }

    pub fn get_count(&self) -> usize {
        self.mapping.len()
    }
}

impl Default for GroupResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for GroupResolver {
    fn clone(&self) -> Self {
        Self {
            mapping: Arc::clone(&self.mapping),
        }
    }
}

unsafe impl Send for GroupResolver {}
unsafe impl Sync for GroupResolver {}
