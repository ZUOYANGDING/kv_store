use crate::error::Result;
use std::io::{BufWriter, Seek, SeekFrom, Write};
/// struct to hold current buffer writer and its position
pub struct BufferWriterWithPosition<W: Write + Seek> {
    pub position: u64,
    pub writer: BufWriter<W>,
}

impl<W: Write + Seek> BufferWriterWithPosition<W> {
    pub fn new(mut inner: W) -> Result<Self> {
        let position = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            position,
            writer: BufWriter::new(inner),
        })
    }
}

impl<W: Write + Seek> Write for BufferWriterWithPosition<W> {
    /// write `buf` into Buffer, update cursor position and return offset
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let offset = self.writer.write(buf)?;
        self.position += offset as u64;
        Ok(offset)
    }

    /// flush the buffer
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// seek to an offset, update cursor position and return it
impl<W: Write + Seek> Seek for BufferWriterWithPosition<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.position = self.writer.seek(pos)?;
        Ok(self.position)
    }
}
