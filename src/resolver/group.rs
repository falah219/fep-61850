use dashmap::DashMap;
use std::sync::Arc;

/// Group `master_data_objects` IEC61850: `initial` (CDC) -> `id_group`, plus `id` baris -> (initial, id_group)
/// supaya MASTER_DATA_OBJECT_EVENT bisa memperbarui / menghapus satu baris tanpa muat ulang semua.
#[derive(Clone, Default)]
pub struct GroupResolver {
    mapping: Arc<DashMap<String, String>>,
    rows: Arc<DashMap<String, (String, String)>>,
}

impl GroupResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// `groups` = (id, initial, id_group).
    pub fn load(&self, groups: Vec<(String, String, String)>) {
        self.mapping.clear();
        self.rows.clear();
        for (id, initial, id_group) in groups {
            self.upsert_row(&id, initial, id_group);
        }
    }

    pub fn get_group(&self, initial: &str) -> Option<String> {
        self.mapping.get(initial).map(|v| v.clone())
    }

    /// true kalau `id` baris master_data_objects IEC61850 yang sedang dipakai.
    pub fn has_row(&self, id: &str) -> bool {
        self.rows.contains_key(id)
    }

    /// Tambah / perbarui satu baris. `initial` berubah -> entri initial lama dihapus.
    pub fn upsert_row(&self, id: &str, initial: String, id_group: String) {
        self.remove_row(id);
        self.mapping.insert(initial.clone(), id_group.clone());
        self.rows.insert(id.to_string(), (initial, id_group));
    }

    /// Hapus satu baris. false kalau `id` tidak dikenal (milik protokol lain).
    // ponytail: dua baris dengan initial sama -> menghapus salah satunya bisa mengosongkan initial itu
    // sampai muat ulang berikutnya; simpan daftar per initial kalau data seperti itu ada
    pub fn remove_row(&self, id: &str) -> bool {
        let Some((_, (initial, id_group))) = self.rows.remove(id) else { return false };
        self.mapping.remove_if(&initial, |_, v| *v == id_group);
        true
    }

    pub fn get_count(&self) -> usize {
        self.mapping.len()
    }
}
