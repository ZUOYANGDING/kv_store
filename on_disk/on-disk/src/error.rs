use failure::Fail;
use std::io;

#[derive(Fail, Debug)]
pub enum KVStoreError {
    // IO error
    #[fail(display = "IO Error {}", _0)]
    Io(#[cause] io::Error),
    // Serialization or deserialization error
    #[fail(display = "Serializtion or deserialization error")]
    Serde(#[cause] serde_json::Error),
    // Removing non-existent key error
    #[fail(display = "Key not found")]
    KeyNotFound,
    // Unexpected command type error
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KVStoreError {
    fn from(err: io::Error) -> KVStoreError {
        KVStoreError::Io(err)
    }
}

impl From<serde_json::Error> for KVStoreError {
    fn from(err: serde_json::Error) -> Self {
        KVStoreError::Serde(err)
    }
}

pub type Result<T> = std::result::Result<T, KVStoreError>;
