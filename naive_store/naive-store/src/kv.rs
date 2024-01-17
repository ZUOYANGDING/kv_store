use std::collections::HashMap;

#[derive(Default)]
pub struct KVStore {
    storage: HashMap<String, String>,
}

impl KVStore {
    pub fn new() -> KVStore {
        KVStore {
            storage: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.storage.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.storage.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        self.storage.remove(&key);
    }
}
