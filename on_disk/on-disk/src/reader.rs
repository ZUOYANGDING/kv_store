use crate::error::Result;
use std::io::{BufReader, Read, Seek};

// struct to hold current reader and its postion
pub struct BufferReaderWithPosition<R: Read + Seek> {
    pub reader: BufReader<R>,
    pub position: u64,
}

/// read the `buf` into buffer, updated cursor position and return offset
impl<R: Read + Seek> Read for BufferReaderWithPosition<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let offset = self.reader.read(buf)?;
        self.position += offset as u64;
        Ok(offset)
    }
}

/// seek to an offset, update cursor position and return it
impl<R: Read + Seek> Seek for BufferReaderWithPosition<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.position = self.reader.seek(pos)?;
        Ok(self.position)
    }
}

impl<R: Read + Seek> BufferReaderWithPosition<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let position = inner.seek(std::io::SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(inner),
            position,
        })
    }
}
