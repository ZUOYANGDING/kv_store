use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::Seek,
    path::{Path, PathBuf},
};

use crate::{command::Command, error::Result, reader};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{
    command::CommandMetaData, reader::BufferReaderWithPosition, writer::BufferWriterWithPosition,
};

pub struct KVStore {
    // abs path to log files
    db_path: PathBuf,
    // readers mapping file_number -> file and offset of file
    readers: HashMap<u64, BufferReaderWithPosition<File>>,
    // writers of the current log
    writer: BufferWriterWithPosition<File>,
    // current file number
    current_file_num: u64,
    // mapping of key ->  CommandMetaData
    index_map: BTreeMap<String, CommandMetaData>,
    // size of data in bytes could be delete when compact
    uncompacted: u64,
}

impl KVStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KVStore> {
        // create dir for files
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers: HashMap<u64, BufferReaderWithPosition<File>> = HashMap::new();
        let mut index_map: BTreeMap<String, CommandMetaData> = BTreeMap::new();

        // get all existing log files
        let existing_file_num_list = sort_file_by_number(&path)?;
        let mut uncompacted = 0_u64;
        // load all existing file
        for file_num in &existing_file_num_list {
            let mut reader = BufferReaderWithPosition::new(File::open(
                build_file_path_by_number(&path, file_num.to_owned()),
            )?)?;
            uncompacted += load_uncompacted_data(file_num.to_owned(), &mut reader, &mut index_map)?;
            readers.insert(file_num.to_owned(), reader);
        }
        // set current_file_num
        let current_file_num = existing_file_num_list.last().unwrap_or(&0) + 1;
        // create current writer and insert into reader cache
        let writer = new_file(&path, current_file_num, &mut readers)?;
        Ok(Self {
            db_path: path,
            readers,
            writer,
            current_file_num,
            index_map,
            uncompacted,
        })
    }
}

/// Go through the log file
///
/// replace old `SET` ComandMetaData with newest `SET` in index_map, and count how many `SET` command and data in bytes can be compacted
///
/// remove the `SET` CommandMetaData by `Remove` Command, and count how many `SET` command and data and `Remove` command itself can be compacted
///
/// return data in bytes that can be compacted in next compact process
fn load_uncompacted_data(
    file_num: u64,
    reader: &mut BufferReaderWithPosition<File>,
    index_map: &mut BTreeMap<String, CommandMetaData>,
) -> Result<u64> {
    // load command from begin of file
    let mut old_position = reader.seek(std::io::SeekFrom::Start(0))?;
    // load and deserialize the command, and trans them into iterator
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompatced = 0_u64;

    // go through all commands
    while let Some(cmd) = stream.next() {
        let new_position = stream.byte_offset() as u64;
        match cmd? {
            Command::Set(key, _) => {
                // get prev red Set command with same input key, put the prev Set command into uncompacted data
                let data_in_bytes = index_map
                    .insert(
                        key,
                        CommandMetaData {
                            file_number: file_num,
                            offset: old_position,
                            length: new_position - old_position,
                        },
                    )
                    .map(|md| md.length)
                    .unwrap_or(0_u64);
                uncompatced += data_in_bytes;
            }
            Command::Remove(key) => {
                let data_in_bytes = index_map.remove(&key).map(|md| md.length).unwrap_or(0);
                uncompatced += data_in_bytes;
                // also add the `Remove` command itself into uncompacted
                uncompatced += new_position - old_position;
            }
        }
        old_position = new_position;
    }
    Ok(uncompatced)
}

fn new_file(
    path: &Path,
    file_num: u64,
    readers: &mut HashMap<u64, BufferReaderWithPosition<File>>,
) -> Result<BufferWriterWithPosition<File>> {
    let file_path = build_file_path_by_number(path, file_num);
    let writer = BufferWriterWithPosition::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_path)?,
    )?;
    readers.insert(
        file_num,
        BufferReaderWithPosition::new(File::open(&file_path)?)?,
    );
    Ok(writer)
}

fn build_file_path_by_number(path: &Path, file_num: u64) -> PathBuf {
    path.join(format!("{}.log", file_num))
}

fn sort_file_by_number(path: &Path) -> Result<Vec<u64>> {
    let mut file_num_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|path_str| path_str.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    file_num_list.sort_unstable();
    Ok(file_num_list)
}
