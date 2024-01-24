//! This is implementation of KVStoreEngine by KVStore DB
use crate::Result;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

pub struct KVStore {
    // path to database
    pub db_path: PathBuf,
    // current data file number
    pub current_file_number: u64,
    // file readers cache
    pub readers: HashMap<u64, BufferReaderWithPosition<File>>,
    // current file writer
    pub current_writer: BuffferWriterWithPosition<File>,
    // newest command cache
    pub index_map: BTreeMap<String, CommandMedaData>,
    // size of uncompacted data in bytes
    pub uncompact: u64,
}

impl KVStore {}

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
pub struct CommandMedaData {
    file_number: u64,
    offset: u64,
    length: u64,
}

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
