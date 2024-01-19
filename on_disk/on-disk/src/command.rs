use serde::{Deserialize, Serialize};
/// struct hold command's meta data (in which log file, offset of this command and length)
pub struct CommandMetaData {
    pub file_number: u64,
    pub offset: u64,
    pub length: u64,
}

/// Command Enum
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    // set command
    Set(String, String),
    // remove commadn
    Remove(String),
}

impl Command {
    pub fn set(key: String, value: String) -> Command {
        Command::Set(key, value)
    }

    pub fn remove(key: String) -> Command {
        Command::Remove(key)
    }
}
