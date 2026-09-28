use std::{
    fmt, fs,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

pub const WAL_HEADER_SIZE: u64 = 32;
pub const FRAME_HEADER_SIZE: u64 = 24;

const WAL_MAGIC: u32 = 0x377f_0682;

#[derive(Clone)]
pub struct WalFrame {
    pub pgno: u32,
    pub data: Vec<u8>,
}

impl fmt::Debug for WalFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFrame")
            .field("pgno", &self.pgno)
            .field("data", &format_args!("[{} bytes]", self.data.len()))
            .finish()
    }
}

#[derive(Debug)]
pub struct WalTransaction {
    pub commit: u32,
    pub page_size: u32,
    pub frames: Vec<WalFrame>,
    pub cursor_after: WalCursor,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalCursor {
    pub offset: u64,
    pub salt: [u8; 8],
    pub checksum: (u32, u32),
    pub big_endian: bool,
}

impl WalCursor {
    pub fn is_initialized(&self) -> bool {
        self.offset >= WAL_HEADER_SIZE
    }
}

pub fn read_committed(
    path: &Path,
    cursor: &mut WalCursor,
) -> Result<Vec<WalTransaction>, WalError> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return vanished(cursor),
        Err(err) => return Err(WalError::Io(err)),
    };

    let len = file.metadata()?.len();

    if len < WAL_HEADER_SIZE {
        return vanished(cursor);
    }

    let header = WalHeader::read(&mut file)?;

    if !cursor.is_initialized() {
        *cursor = WalCursor {
            offset: WAL_HEADER_SIZE,
            salt: header.salt,
            checksum: header.checksum,
            big_endian: header.big_endian,
        };
    } else if cursor.salt != header.salt {
        return Err(WalError::GenerationChanged);
    } else if len < cursor.offset {
        return Err(WalError::GenerationChanged);
    }

    let page_size = header.page_size as u64;
    let frame_size = FRAME_HEADER_SIZE + page_size;

    let mut transactions = Vec::new();
    let mut pending: Vec<WalFrame> = Vec::new();
    let mut offset = cursor.offset;
    let mut checksum = cursor.checksum;

    file.seek(SeekFrom::Start(offset))?;

    while offset + frame_size <= len {
        let mut frame_header = [0u8; FRAME_HEADER_SIZE as usize];
        file.read_exact(&mut frame_header)?;

        let mut data = vec![0u8; header.page_size as usize];
        file.read_exact(&mut data)?;

        if frame_header[8..16] != header.salt {
            break;
        }

        let expected = (
            u32::from_be_bytes(frame_header[16..20].try_into().unwrap()),
            u32::from_be_bytes(frame_header[20..24].try_into().unwrap()),
        );

        let running = checksum_bytes(checksum, &frame_header[0..8], header.big_endian);
        let running = checksum_bytes(running, &data, header.big_endian);

        if running != expected {
            break;
        }

        let pgno = u32::from_be_bytes(frame_header[0..4].try_into().unwrap());
        let commit = u32::from_be_bytes(frame_header[4..8].try_into().unwrap());

        if pgno == 0 {
            return Err(WalError::ZeroPageNumber { offset });
        }

        checksum = running;
        offset += frame_size;

        pending.push(WalFrame { pgno, data });

        if commit != 0 {
            transactions.push(WalTransaction {
                commit,
                page_size: header.page_size,
                frames: std::mem::take(&mut pending),
                cursor_after: WalCursor {
                    offset,
                    salt: header.salt,
                    checksum,
                    big_endian: header.big_endian,
                },
            });
        }
    }

    Ok(transactions)
}

fn vanished(cursor: &WalCursor) -> Result<Vec<WalTransaction>, WalError> {
    if cursor.is_initialized() {
        Err(WalError::GenerationChanged)
    } else {
        Ok(Vec::new())
    }
}

#[derive(Debug)]
struct WalHeader {
    page_size: u32,
    salt: [u8; 8],
    checksum: (u32, u32),
    big_endian: bool,
}

impl WalHeader {
    fn read<R>(mut r: R) -> Result<Self, WalError>
    where
        R: Read,
    {
        let mut buf = [0u8; WAL_HEADER_SIZE as usize];
        r.read_exact(&mut buf)?;

        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());

        if magic & !1 != WAL_MAGIC {
            return Err(WalError::Magic(magic));
        }

        let big_endian = magic & 1 == 1;

        let page_size = u32::from_be_bytes(buf[8..12].try_into().unwrap());

        if !is_valid_page_size(page_size) {
            return Err(WalError::PageSize(page_size));
        }

        let expected = (
            u32::from_be_bytes(buf[24..28].try_into().unwrap()),
            u32::from_be_bytes(buf[28..32].try_into().unwrap()),
        );

        let checksum = checksum_bytes((0, 0), &buf[0..24], big_endian);

        if checksum != expected {
            return Err(WalError::HeaderChecksum);
        }

        Ok(Self {
            page_size,
            salt: buf[16..24].try_into().unwrap(),
            checksum,
            big_endian,
        })
    }
}

fn is_valid_page_size(page_size: u32) -> bool {
    (512..=65536).contains(&page_size) && page_size.is_power_of_two()
}

fn checksum_bytes(seed: (u32, u32), data: &[u8], big_endian: bool) -> (u32, u32) {
    let (mut s0, mut s1) = seed;

    for chunk in data.chunks_exact(8) {
        let (a, b) = if big_endian {
            (
                u32::from_be_bytes(chunk[0..4].try_into().unwrap()),
                u32::from_be_bytes(chunk[4..8].try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
            )
        };

        s0 = s0.wrapping_add(a).wrapping_add(s1);
        s1 = s1.wrapping_add(b).wrapping_add(s0);
    }

    (s0, s1)
}

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Magic(u32),
    PageSize(u32),
    HeaderChecksum,
    ZeroPageNumber { offset: u64 },
    GenerationChanged,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => {
                write!(f, "wal io error: {err}")
            }
            Self::Magic(magic) => {
                write!(f, "not a sqlite wal: bad magic {magic:#010x}")
            }
            Self::PageSize(page_size) => {
                write!(f, "unsupported wal page size: {page_size}")
            }
            Self::HeaderChecksum => {
                write!(f, "wal header checksum mismatch")
            }
            Self::ZeroPageNumber { offset } => {
                write!(f, "wal frame at offset {offset} has page number zero")
            }
            Self::GenerationChanged => {
                write!(
                    f,
                    "the wal was restarted or truncated: capture must be re-attached"
                )
            }
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
