use crate::format::{CRC, require};
use std::io::{self, Read, Write};

pub(crate) struct Stream<T> {
    pub inner: T,
    pub offset: u64,
    pub hash: crc::Digest<'static, u64>,
}

impl<T> Stream<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            offset: 0,
            hash: CRC.digest(),
        }
    }
}

impl<R: Read> Stream<R> {
    pub fn field<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut bytes = [0; N];
        self.read_exact(&mut bytes)?;
        self.hash.update(&bytes);
        Ok(bytes)
    }

    pub fn varint(&mut self) -> io::Result<u64> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.field::<1>()?[0];
            require(shift != 63 || byte <= 1, "page index varint overflow")?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        unreachable!()
    }
}

impl<R: Read> Read for Stream<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.offset += count as u64;
        Ok(count)
    }
}

impl<W: Write> Stream<W> {
    pub fn field_write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_all(bytes)?;
        self.hash.update(bytes);
        Ok(())
    }

    pub fn varint_write(&mut self, mut value: u64) -> io::Result<()> {
        while value >= 128 {
            self.field_write(&[(value as u8) | 0x80])?;
            value >>= 7;
        }
        self.field_write(&[value as u8])
    }
}

impl<W: Write> Write for Stream<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.offset += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
