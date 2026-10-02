use std::io;

pub const NO_CHECKSUM: u32 = 2;
pub const CHECKSUM_FLAG: u64 = 1 << 63;
pub(crate) const CRC: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_GO_ISO);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub flags: u32,
    pub page_size: u32,
    pub commit: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    pub timestamp: i64,
    pub pre_apply_checksum: u64,
    pub wal_offset: i64,
    pub wal_size: i64,
    pub wal_salt: [u32; 2],
    pub node_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Page {
    pub number: u32,
    pub data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trailer {
    pub post_apply_checksum: u64,
    pub file_checksum: u64,
}

impl Header {
    pub fn validate(&self) -> io::Result<()> {
        require(self.flags & !NO_CHECKSUM == 0, "unknown header flags")?;
        require(
            (512..=65536).contains(&self.page_size) && self.page_size.is_power_of_two(),
            "invalid page size",
        )?;
        require(
            self.min_txid > 0 && self.min_txid <= self.max_txid,
            "invalid transaction range",
        )?;
        require(
            self.wal_offset >= 0 && self.wal_size >= 0,
            "negative WAL position",
        )?;
        require(
            self.wal_offset != 0 || (self.wal_size == 0 && self.wal_salt == [0, 0]),
            "missing WAL offset",
        )?;
        if self.min_txid == 1 || self.flags & NO_CHECKSUM != 0 {
            require(
                self.pre_apply_checksum == 0,
                "unexpected pre-apply checksum",
            )
        } else {
            require(
                self.pre_apply_checksum & CHECKSUM_FLAG != 0,
                "missing pre-apply checksum",
            )
        }
    }

    pub(crate) fn parse(bytes: &[u8; 100]) -> io::Result<Self> {
        require(&bytes[..4] == b"LTX1", "invalid LTX magic")?;
        let word = |offset| u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let long = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let header = Self {
            flags: word(4),
            page_size: word(8),
            commit: word(12),
            min_txid: long(16),
            max_txid: long(24),
            timestamp: long(32) as i64,
            pre_apply_checksum: long(40),
            wal_offset: long(48) as i64,
            wal_size: long(56) as i64,
            wal_salt: [word(64), word(68)],
            node_id: long(72),
        };
        header.validate()?;
        Ok(header)
    }

    pub(crate) fn bytes(&self) -> [u8; 100] {
        let mut bytes = [0; 100];
        bytes[..4].copy_from_slice(b"LTX1");
        for (offset, value) in [
            (4, self.flags),
            (8, self.page_size),
            (12, self.commit),
            (64, self.wal_salt[0]),
            (68, self.wal_salt[1]),
        ] {
            bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        }
        for (offset, value) in [
            (16, self.min_txid),
            (24, self.max_txid),
            (32, self.timestamp as u64),
            (40, self.pre_apply_checksum),
            (48, self.wal_offset as u64),
            (56, self.wal_size as u64),
            (72, self.node_id),
        ] {
            bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    pub(crate) fn page(&self, previous: u32, number: u32) -> io::Result<()> {
        let lock = self.lock_page();
        require(
            number > previous && number <= self.commit && number != lock,
            "invalid page order or number",
        )?;
        if self.min_txid == 1 {
            require(
                number == previous + if previous == lock - 1 { 2 } else { 1 },
                "snapshot has a missing page",
            )?;
        }
        Ok(())
    }

    pub(crate) fn complete(&self, last: u32) -> io::Result<()> {
        let end = self.commit - u32::from(self.commit == self.lock_page());
        require(self.min_txid != 1 || last == end, "incomplete snapshot")
    }

    pub(crate) fn checksum(&self, post: u64, calculated: u64) -> io::Result<()> {
        if self.flags & NO_CHECKSUM != 0 {
            require(post == 0, "unexpected post-apply checksum")
        } else {
            require(post & CHECKSUM_FLAG != 0, "missing post-apply checksum")?;
            require(
                self.min_txid != 1 || post == calculated,
                "snapshot checksum mismatch",
            )
        }
    }

    fn lock_page(&self) -> u32 {
        0x40000000 / self.page_size + 1
    }
}

pub(crate) fn page_checksum(number: u32, data: &[u8]) -> u64 {
    let mut hash = CRC.digest();
    hash.update(&number.to_be_bytes());
    hash.update(data);
    hash.finalize() | CHECKSUM_FLAG
}

pub(crate) fn require(condition: bool, message: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, message))
    }
}
