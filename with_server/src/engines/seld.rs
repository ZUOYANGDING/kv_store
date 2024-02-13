//! This is implementation of KVStoreEngine by sled DB

use super::KVStoreEngine;
use crate::error::{KVStoreError, Result};
use sled::{Db, Tree};

#[derive(Clone)]
pub struct SledKVStore(Db);

impl SledKVStore {
    pub fn open(db: Db) -> Self {
        SledKVStore(db)
    }
}

impl KVStoreEngine for SledKVStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.remove(key)?.ok_or(KVStoreError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}
