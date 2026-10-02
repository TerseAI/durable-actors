use crate::{
    Header,
    format::{CHECKSUM_FLAG, NO_CHECKSUM, page_checksum, require},
    stream::Stream,
};
use std::io::{self, Write};

pub struct Encoder<W> {
    output: Stream<W>,
    header: Header,
    index: Vec<(u32, u64, u64)>,
    last_page: u32,
    checksum: u64,
}

impl<W: Write> Encoder<W> {
    pub fn new(writer: W, header: Header) -> io::Result<Self> {
        header.validate()?;
        let mut output = Stream::new(writer);
        output.field_write(&header.bytes())?;
        Ok(Self {
            output,
            header,
            index: Vec::new(),
            last_page: 0,
            checksum: CHECKSUM_FLAG,
        })
    }

    pub fn write_page(&mut self, number: u32, data: &[u8]) -> io::Result<()> {
        self.header.page(self.last_page, number)?;
        require(
            data.len() == self.header.page_size as usize,
            "incorrect page size",
        )?;
        let compressed = lz4_flex::block::compress(data);
        let offset = self.output.offset;
        self.output.field_write(&number.to_be_bytes())?;
        self.output.field_write(&1u16.to_be_bytes())?;
        self.output
            .field_write(&(compressed.len() as u32).to_be_bytes())?;
        self.output.write_all(&compressed)?;
        // LTX hashes the uncompressed page, but its on-disk header and size prefix.
        self.output.hash.update(data);
        if self.header.min_txid == 1 && self.header.flags & NO_CHECKSUM == 0 {
            self.checksum = CHECKSUM_FLAG | (self.checksum ^ page_checksum(number, data));
        }
        self.index
            .push((number, offset, self.output.offset - offset));
        self.last_page = number;
        Ok(())
    }

    pub fn finish(mut self, post_apply_checksum: u64) -> io::Result<W> {
        self.header.complete(self.last_page)?;
        self.header.checksum(post_apply_checksum, self.checksum)?;
        self.output.field_write(&[0; 6])?;
        self.write_index()?;
        self.output
            .field_write(&post_apply_checksum.to_be_bytes())?;
        let checksum = self.output.hash.clone().finalize() | CHECKSUM_FLAG;
        self.output.write_all(&checksum.to_be_bytes())?;
        Ok(self.output.inner)
    }

    fn write_index(&mut self) -> io::Result<()> {
        let start = self.output.offset;
        for &(number, offset, size) in &self.index {
            self.output.varint_write(u64::from(number))?;
            self.output.varint_write(offset)?;
            self.output.varint_write(size)?;
        }
        self.output.varint_write(0)?;
        let size = self.output.offset - start;
        self.output.field_write(&size.to_be_bytes())
    }
}
