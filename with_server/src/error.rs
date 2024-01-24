use failure::Fail;
use std::io;
use std::string::FromUtf8Error;

#[derive(Fail, Debug)]
pub enum KVStoreError {
    // IO error
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] io::Error),
    // Serde error
    #[fail(display = "serde_json error: {}", _0)]
    Serde(#[cause] serde_json::Error),
    // Sled DB error
    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),
    // Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),
    // Removing non-existent key error
    #[fail(display = "Key not found")]
    KeyNotFound,
    // Invalid Command type error
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
    // Other message in String
    #[fail(display = "{}", _0)]
    Other(String),
}

impl From<io::Error> for KVStoreError {
    fn from(err: io::Error) -> Self {
        KVStoreError::Io(err)
    }
}

impl From<serde_json::Error> for KVStoreError {
    fn from(err: serde_json::Error) -> Self {
        KVStoreError::Serde(err)
    }
}

impl From<sled::Error> for KVStoreError {
    fn from(err: sled::Error) -> Self {
        KVStoreError::Sled(err)
    }
}

impl From<FromUtf8Error> for KVStoreError {
    fn from(err: FromUtf8Error) -> Self {
        KVStoreError::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, KVStoreError>;
