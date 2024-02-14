//! This is implementation of KVStoreEngine by KVStore DB

use serde::{de::value, Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KVStoreEngine, Result};
use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, read, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KVStore {
    // path to database
    pub db_path: PathBuf,
    // current data file number
    pub current_file_number: u64,
    // file readers cache
    pub readers: HashMap<u64, BufferReaderWithPosition<File>>,
    // current file writer
    pub current_writer: BuffferWriterWithPosition<File>,
    // newest command cache (only cache `SET` command)
    pub index_map: BTreeMap<String, CommandMedaData>,
    // size of uncompacted data in bytes
    pub uncompact: u64,
}

impl KVStore {
    /// open the existing db files.
    /// load exsiting readers
    /// load most recent writer
    /// load most recent command into index_map and uncompacted data in bytes
    pub fn open(path: impl Into<PathBuf>) -> Result<KVStore> {
        // open existing db by input path
        let path = path.into();
        fs::create_dir_all(&path)?;
        let mut readers: HashMap<u64, BufferReaderWithPosition<File>> = HashMap::new();
        let mut index_map: BTreeMap<String, CommandMedaData> = BTreeMap::new();

        let file_num_list = sort_file_by_number(&path)?;
        let mut uncompact = 0_u64;
        // load uncompacted data, and update readers' map
        for file_num in &file_num_list {
            let file_path: PathBuf = build_file_path_by_number(&path, file_num.to_owned());
            let mut file = BufferReaderWithPosition::new(File::open(file_path)?)?;
            uncompact += load_uncompacted_data(file_num.to_owned(), &mut file, &mut index_map)?;
            // insert file into readers's map
            readers.insert(file_num.to_owned(), file);
        }
        let current_file_number = file_num_list.last().unwrap_or(&0) + 1;
        let current_writer = new_file(&path, current_file_number, &mut readers)?;
        Ok(KVStore {
            db_path: path,
            current_file_number,
            readers,
            current_writer,
            index_map,
            uncompact,
        })
    }

    /// compact uncompacted data
    pub fn compact(&mut self) -> Result<()> {
        // create a new file to store data after compacted
        let compact_file_number = self.current_file_number + 1;
        let mut compact_writer =
            self::new_file(&self.db_path, compact_file_number, &mut self.readers)?;
        let mut offset = 0_u64;
        for command_meta_data in self.index_map.values_mut() {
            // get the reader file by file number
            let reader = self
                .readers
                .get_mut(&command_meta_data.file_number)
                .expect("cannot find matched reader");
            // seek to the command
            reader.seek(std::io::SeekFrom::Start(command_meta_data.offset))?;
            // get command
            let mut command = reader.take(command_meta_data.length);
            // write into writer
            io::copy(&mut command, &mut compact_writer)?;
            // updated the CommandMetaData in index_map by the CommandMetaData in compact file
            *command_meta_data = CommandMedaData {
                file_number: compact_file_number,
                length: compact_writer.position - offset,
                offset,
            };
            // update offset
            offset = compact_writer.position;
        }
        compact_writer.flush()?;
        // delete the compacted files
        let compacted_file_number_list: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&file_num| file_num < compact_file_number)
            .cloned()
            .collect();
        for file_num in compacted_file_number_list {
            self.readers.remove(&file_num);
            fs::remove_file(build_file_path_by_number(&self.db_path, file_num))?;
        }
        self.current_file_number = compact_file_number + 1;
        self.current_writer =
            self::new_file(&self.db_path, self.current_file_number, &mut self.readers)?;
        self.uncompact = 0_u64;
        Ok(())
    }
}

impl KVStoreEngine for KVStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key.to_owned(), value);
        let offset = self.current_writer.position;
        serde_json::to_writer(&mut self.current_writer, &command)?;
        let command_length = self.current_writer.position - offset;
        let old_data = self.index_map.insert(
            key.to_owned(),
            CommandMedaData {
                file_number: self.current_file_number,
                offset,
                length: command_length,
            },
        );
        self.uncompact += old_data.map(|cmd| cmd.length).unwrap_or(0_u64);
        self.current_writer.flush()?;
        if self.uncompact > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(command_meta_data) = self.index_map.get(&key) {
            // get reader by CommandMetaData
            let reader = self
                .readers
                .get_mut(&command_meta_data.file_number)
                .expect("cannot find matched reader");
            // seek to command position
            reader.seek(io::SeekFrom::Start(command_meta_data.offset))?;
            // get the data
            let data = reader.take(command_meta_data.length);
            if let Command::Set(_, value) = serde_json::from_reader(data)? {
                Ok(Some(value))
            } else {
                Err(crate::KVStoreError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.contains_key(&key) {
            let command_meta_data = self.index_map.remove(&key);
            self.uncompact += command_meta_data.map(|cmd| cmd.length).unwrap_or(0);
            // create and write the Remove command into current writer file
            let command = Command::rm(key);
            let offset = self.current_writer.position;
            serde_json::to_writer(&mut self.current_writer, &command)?;
            let data_length = self.current_writer.position - offset;
            // add the remove command into uncompact data
            self.uncompact += data_length;
            self.current_writer.flush()?;
            if self.uncompact > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(crate::KVStoreError::KeyNotFound)
        }
    }
}

/// open/create a new file
///
/// create a BufferReaderWithPosition for this file and put it into the reader cache
///
/// return a BufferWriterWithPosition with the created/open file
fn new_file(
    dir_path: &Path,
    file_num: u64,
    readers: &mut HashMap<u64, BufferReaderWithPosition<File>>,
) -> Result<BuffferWriterWithPosition<File>> {
    let file_path = build_file_path_by_number(dir_path, file_num);
    let writer = BuffferWriterWithPosition::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&file_path)?,
    )?;
    readers.insert(
        file_num,
        BufferReaderWithPosition::new(File::open(&file_path)?)?,
    );
    Ok(writer)
}

/// Go through the log file
///
/// replace old `SET` ComandMetaData with newest `SET` in index_map, and count how many `SET` command and data in bytes can be compacted
///
/// remove the `SET` CommandMetaData by `Remove` Command, and count how many `SET` command and data and `Remove` command itself can be compacted
///
/// return data in bytes that can be compacted in next compact process
fn load_uncompacted_data(
    file_number: u64,
    file: &mut BufferReaderWithPosition<File>,
    index_map: &mut BTreeMap<String, CommandMedaData>,
) -> Result<u64> {
    let mut data_in_bytes = 0_u64;
    // read from begining
    let mut old_offset = file.seek(std::io::SeekFrom::Start(0))?;
    // read and load the file into Iterator<Command>
    let mut commands = Deserializer::from_reader(file).into_iter::<Command>();

    while let Some(command) = commands.next() {
        let new_offset = commands.byte_offset() as u64;
        match command? {
            Command::Set(key, _) => {
                let old_data = index_map.insert(
                    key,
                    CommandMedaData {
                        file_number,
                        offset: old_offset,
                        length: new_offset - old_offset,
                    },
                );
                // add the length of prev `set` with the same input key command as uncompacted data
                data_in_bytes += old_data.map(|cmd| cmd.length).unwrap_or(0);
            }
            Command::Remove(key) => {
                let old_data = index_map.remove(&key);
                // add the removed `set` with input key command as uncompacted data
                data_in_bytes += old_data.map(|cmd| cmd.length).unwrap_or(0);
                // add the `remove` command itself as uncompacted data
                data_in_bytes += new_offset - old_offset;
            }
        }
        old_offset = new_offset;
    }
    Ok(data_in_bytes)
}

/// build file path
fn build_file_path_by_number(dir_path: &Path, file_number: u64) -> PathBuf {
    dir_path.join(format!("{}.log", file_number))
}

/// sort the file by its number
fn sort_file_by_number(dir_path: &Path) -> Result<Vec<u64>> {
    let mut file_num_list: Vec<u64> = fs::read_dir(dir_path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|file_path| file_path.is_file() && file_path.extension() == Some("log".as_ref()))
        .flat_map(|file_name| {
            file_name
                .file_name()
                .and_then(OsStr::to_str)
                .map(|file_str| file_str.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    file_num_list.sort_unstable();
    Ok(file_num_list)
}

/// Reader with buffer
pub struct BufferReaderWithPosition<R: Read + Seek> {
    reader: BufReader<R>,
    position: u64,
}

impl<R: Read + Seek> Read for BufferReaderWithPosition<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let offset = self.reader.read(buf)?;
        self.position += offset as u64;
        Ok(offset)
    }
}

impl<R: Read + Seek> Seek for BufferReaderWithPosition<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.position = self.reader.seek(pos)?;
        Ok(self.position)
    }
}

impl<R: Read + Seek> BufferReaderWithPosition<R> {
    fn new(mut inner: R) -> Result<Self> {
        let position = inner.seek(std::io::SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(inner),
            position,
        })
    }
}

/// writer with buffer
pub struct BuffferWriterWithPosition<W: Write + Seek> {
    writer: BufWriter<W>,
    position: u64,
}

impl<W: Write + Seek> Write for BuffferWriterWithPosition<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let offset = self.writer.write(buf)?;
        self.position += offset as u64;
        Ok(offset)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BuffferWriterWithPosition<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.position = self.writer.seek(pos)?;
        Ok(self.position)
    }
}

impl<W: Write + Seek> BuffferWriterWithPosition<W> {
    fn new(mut inner: W) -> Result<Self> {
        let position = inner.seek(std::io::SeekFrom::Current(0))?;
        Ok(Self {
            writer: BufWriter::new(inner),
            position,
        })
    }
}

/// command's meta data, offset of a command and length of the command/command with data
#[derive(Deserialize, Serialize)]
pub struct CommandMedaData {
    file_number: u64,
    offset: u64,
    length: u64,
}

#[derive(Deserialize, Serialize)]
enum Command {
    Set(String, String),
    Remove(String),
}
impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set(key, value)
    }

    fn rm(key: String) -> Command {
        Command::Remove(key)
    }
}
