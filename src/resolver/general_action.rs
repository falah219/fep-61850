use dashmap::DashMap;
use std::sync::Arc;

pub struct GeneralActionResolver {
    mapping: Arc<DashMap<String, String>>,
}

impl GeneralActionResolver {
    pub fn new() -> Self {
        Self {
            mapping: Arc::new(DashMap::new()),
        }
    }

    pub fn load(&self, actions: Vec<(String, i32)>) {
        self.mapping.clear();
        for (name_action, code) in actions {
            self.mapping.insert(name_action, code.to_string());
        }
    }

    pub fn get_code(&self, action: &str) -> Option<String> {
        self.mapping.get(action).map(|v| v.clone())
    }

    pub fn add_action(&self, action: String, code: String) {
        self.mapping.insert(action, code);
    }

    pub fn remove_action(&self, action: &str) {
        self.mapping.remove(action);
    }

    pub fn reload(&self) {
        self.mapping.clear();
    }

    pub fn get_count(&self) -> usize {
        self.mapping.len()
    }
}

impl Default for GeneralActionResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for GeneralActionResolver {
    fn clone(&self) -> Self {
        Self {
            mapping: Arc::clone(&self.mapping),
        }
    }
}

unsafe impl Send for GeneralActionResolver {}
unsafe impl Sync for GeneralActionResolver {}
