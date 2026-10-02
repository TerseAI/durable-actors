use crate::{
    Header, Page, Trailer,
    format::{CHECKSUM_FLAG, NO_CHECKSUM, page_checksum, require},
    stream::Stream,
};
use std::io::{self, Read};

pub struct Decoder<R> {
    input: Stream<R>,
    header: Header,
    trailer: Option<Trailer>,
    index: Vec<(u32, u64, u64)>,
    last_page: u32,
    checksum: u64,
}

impl<R: Read> Decoder<R> {
    pub fn new(reader: R) -> io::Result<Self> {
        let mut input = Stream::new(reader);
        let header = Header::parse(&input.field()?)?;
        Ok(Self {
            input,
            header,
            trailer: None,
            index: Vec::new(),
            last_page: 0,
            checksum: CHECKSUM_FLAG,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn trailer(&self) -> Option<Trailer> {
        self.trailer
    }

    pub fn next_page(&mut self) -> io::Result<Option<Page>> {
        if self.trailer.is_some() {
            return Ok(None);
        }
        let offset = self.input.offset;
        let number = u32::from_be_bytes(self.input.field()?);
        let flags = u16::from_be_bytes(self.input.field()?);
        if number == 0 && flags == 0 {
            self.finish()?;
            return Ok(None);
        }
        require(flags & !1 == 0, "unknown page flags")?;
        self.header.page(self.last_page, number)?;
        let data = self.read_data(flags)?;
        self.input.hash.update(&data);
        if self.header.min_txid == 1 && self.header.flags & NO_CHECKSUM == 0 {
            self.checksum = CHECKSUM_FLAG | (self.checksum ^ page_checksum(number, &data));
        }
        self.index
            .push((number, offset, self.input.offset - offset));
        self.last_page = number;
        Ok(Some(Page { number, data }))
    }

    fn read_data(&mut self, flags: u16) -> io::Result<Vec<u8>> {
        let mut data = vec![0; self.header.page_size as usize];
        if flags & 1 != 0 {
            let length = u32::from_be_bytes(self.input.field()?) as usize;
            require(
                length > 0 && length <= lz4_flex::block::get_maximum_output_size(data.len()),
                "invalid compressed page size",
            )?;
            let mut compressed = vec![0; length];
            self.input.read_exact(&mut compressed)?;
            let size = lz4_flex::block::decompress_into(&compressed, &mut data)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            require(size == data.len(), "incorrect decompressed page size")?;
        } else {
            let mut frame = lz4_flex::frame::FrameDecoder::new(&mut self.input);
            frame.read_exact(&mut data)?;
            require(frame.read(&mut [0])? == 0, "oversized LZ4 frame")?;
        }
        Ok(data)
    }

    fn finish(&mut self) -> io::Result<()> {
        self.header.complete(self.last_page)?;
        self.read_index()?;
        let post_apply_checksum = u64::from_be_bytes(self.input.field()?);
        let mut bytes = [0; 8];
        self.input.read_exact(&mut bytes)?;
        let file_checksum = u64::from_be_bytes(bytes);
        require(
            file_checksum == (self.input.hash.clone().finalize() | CHECKSUM_FLAG),
            "file checksum mismatch",
        )?;
        self.header.checksum(post_apply_checksum, self.checksum)?;
        require(
            self.input.read(&mut [0])? == 0,
            "trailing bytes after LTX trailer",
        )?;
        self.trailer = Some(Trailer {
            post_apply_checksum,
            file_checksum,
        });
        Ok(())
    }

    fn read_index(&mut self) -> io::Result<()> {
        let start = self.input.offset;
        for &(number, offset, size) in &self.index {
            require(
                self.input.varint()? == u64::from(number),
                "page index number mismatch",
            )?;
            require(self.input.varint()? == offset, "page index offset mismatch")?;
            require(self.input.varint()? == size, "page index length mismatch")?;
        }
        require(self.input.varint()? == 0, "extra page index entry")?;
        let size = self.input.offset - start;
        require(
            u64::from_be_bytes(self.input.field()?) == size,
            "page index size mismatch",
        )
    }
}
