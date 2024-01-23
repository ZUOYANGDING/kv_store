use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{
    command::Command,
    error::{KVStoreError, Result},
    reader,
};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{
    command::CommandMetaData, reader::BufferReaderWithPosition, writer::BufferWriterWithPosition,
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

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
    /// open the existing db files.
    /// load exsiting readers
    /// load most recent writer
    /// load most recent command into index_map and uncompacted data in bytes
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

    /// set <key, value>
    /// if key already exists, value will be overwritten by the input one
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // create set command
        let command = Command::Set(key, value);
        // get writer's position before write, which will be the offset(start point) of the current command
        let prev_pos = self.writer.position();
        // serialize the command and write it into current writer's buffer
        serde_json::to_writer(&mut self.writer, &command)?;
        // get length of input data in data file
        let data_length = self.writer.position() - prev_pos;
        // update index_map and uncompacted data
        if let Command::Set(key, _) = command {
            self.uncompacted += self
                .index_map
                .insert(
                    key,
                    CommandMetaData {
                        file_number: self.current_file_num,
                        offset: prev_pos,
                        length: data_length,
                    },
                )
                .map(|md| md.length)
                .unwrap_or(0_u64);
        }
        // flush the current writer's buffer
        self.writer.flush()?;
        // check if need compact
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// get value by input key
    /// None if the key does not exist
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // get command meta data
        if let Some(command_meta_data) = self.index_map.get(&key) {
            // reader in target file
            let source_reader = self
                .readers
                .get_mut(&command_meta_data.file_number)
                .expect("cannot get reader");
            // seek to the start postion of the command
            source_reader.seek(std::io::SeekFrom::Start(command_meta_data.offset))?;
            let data_reader = source_reader.take(command_meta_data.length);
            if let Command::Set(_, value) = serde_json::from_reader(data_reader)? {
                Ok(Some(value))
            } else {
                Err(KVStoreError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// remove the key if exist
    /// write the remove command into log file
    /// update uncompacted data (include the old `set` and this `remove`)
    pub fn remove(&mut self, key: String) -> Result<()> {
        // check if key exists
        if self.index_map.contains_key(&key) {
            // remove command and data from index_map, and update uncompacted data
            self.uncompacted += self
                .index_map
                .remove(&key)
                .map(|md| md.length)
                .unwrap_or(0_u64);
            // create a remove command
            let command = Command::Remove(key);
            // get the current writer's postion as offset(start point)
            let prev_pos = self.writer.position();
            // write the remove command
            serde_json::to_writer(&mut self.writer, &command)?;
            // get remove command length
            let data_length = self.writer.position() - prev_pos;
            // update uncompated data
            self.uncompacted += data_length;
            self.writer.flush()?;
            // check if need compact
            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KVStoreError::KeyNotFound)
        }
    }

    /// compact uncompact data to a compact file
    pub fn compact(&mut self) -> Result<()> {
        // increase the current file number by 1 to create a compact file
        let compact_file_num = self.current_file_num + 1;
        let mut compact_writer =
            self::new_file(&self.db_path, compact_file_num, &mut self.readers)?;
        // offset before write in compact file
        let mut prev_offset = 0_u64;
        // start to write compact file
        for command_meta_data in self.index_map.values_mut() {
            // get the reader
            let reader = self
                .readers
                .get_mut(&command_meta_data.file_number)
                .expect("cannot get reader");
            // seek to command position
            reader.seek(std::io::SeekFrom::Start(command_meta_data.offset))?;
            // read the command and data into writer
            let mut command_entry = reader.take(command_meta_data.length);
            io::copy(&mut command_entry, &mut compact_writer)?;
            // replace the current command meta data by the new meta data in compact file
            *command_meta_data = CommandMetaData {
                offset: prev_offset,
                length: compact_writer.position() - prev_offset,
                file_number: compact_file_num,
            };
            // update offset position
            prev_offset = compact_writer.position();
        }
        // flush the compact writer
        compact_writer.flush()?;
        // collect file number that has been
        let file_num_vec: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&file_number| file_number < compact_file_num)
            .cloned()
            .collect();
        // delete collected files
        for file_num in file_num_vec {
            self.readers.remove(&file_num);
            fs::remove_file(build_file_path_by_number(&self.db_path, file_num))?;
        }
        // update current file number and current writer
        self.current_file_num += 2;
        self.writer = new_file(&self.db_path, self.current_file_num, &mut self.readers)?;
        // reset the uncompated data size
        self.uncompacted = 0_u64;
        Ok(())
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

/// open/create a new file
///
/// create a BufferReaderWithPosition for this file and put it into the reader cache
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

/// create file path
fn build_file_path_by_number(path: &Path, file_num: u64) -> PathBuf {
    path.join(format!("{}.log", file_num))
}

/// sort the file by its number
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
