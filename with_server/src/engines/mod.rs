use crate::Result;
pub trait KVStoreEngine {
    /// set key, value
    ///
    /// if key exists, overwrite the value
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// get value by key
    ///
    /// return None if the key does not exists
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// remove key
    ///
    /// return KVStoreError::KeyNotFound if the key does not exsits
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod seld;
