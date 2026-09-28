mod wal;

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use litetx::{
    Checksum, Decoder, Encoder, Header, HeaderFlags, PageChecksum, PageNum, PageSize, TXID,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    time::SystemTime,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqliteState {
    pub txid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal: Option<SqliteWal>,
}

impl PartialEq for SqliteState {
    fn eq(&self, other: &Self) -> bool {
        self.txid == other.txid
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqliteWal {
    pub base_txid: u64,
    pub data: String,
}

pub(crate) struct SqliteCapture {
    directory: tempfile::TempDir,
    txid: u64,
    page_size: Option<PageSize>,
    checksums: BTreeMap<u32, Checksum>,
    checksum: Checksum,
    segment_count: usize,
    segment_bytes: u64,
}

impl SqliteCapture {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            directory: tempfile::tempdir()?,
            txid: 0,
            page_size: None,
            checksums: BTreeMap::new(),
            checksum: Checksum::new(0),
            segment_count: 0,
            segment_bytes: 0,
        })
    }

    pub(crate) fn path(&self) -> std::path::PathBuf {
        self.directory.path().join("actor.sqlite")
    }

    pub(crate) fn txid(&self) -> u64 {
        self.txid
    }

    pub(crate) fn state(&self) -> SqliteState {
        SqliteState {
            txid: self.txid,
            path: Some(self.path().to_string_lossy().into_owned()),
            wal: None,
        }
    }

    pub(crate) fn capture(&mut self, state: &SqliteState) -> Result<Vec<u8>> {
        ensure!(state.txid > self.txid, "SQLite transaction did not advance");
        let wal = state
            .wal
            .as_ref()
            .context("SQLite change is missing its WAL")?;
        ensure!(wal.base_txid <= self.txid, "SQLite WAL history is missing");
        let bytes = STANDARD.decode(&wal.data).context("decode SQLite WAL")?;
        let path = self.directory.path().join("capture.wal");
        fs::write(&path, &bytes)?;
        let transactions = wal::read_committed(&path, &mut wal::WalCursor::default())?;
        ensure!(
            wal.base_txid.checked_add(transactions.len() as u64) == Some(state.txid),
            "SQLite WAL transaction range mismatch"
        );
        ensure!(
            transactions
                .last()
                .is_some_and(|tx| tx.cursor_after.offset == bytes.len() as u64),
            "incomplete or corrupt SQLite WAL"
        );
        let mut pages = BTreeMap::new();
        let mut commit = 0;
        let mut page_size = None;
        for tx in transactions
            .iter()
            .skip((self.txid - wal.base_txid) as usize)
        {
            let size = PageSize::new(tx.page_size)?;
            ensure!(
                self.page_size.is_none_or(|previous| previous == size),
                "SQLite page size changed"
            );
            ensure!(
                page_size.is_none_or(|previous| previous == size),
                "SQLite WAL page size changed"
            );
            page_size = Some(size);
            commit = tx.commit;
            for frame in &tx.frames {
                pages.insert(frame.pgno, frame.data.clone());
            }
            pages.retain(|page, _| *page <= commit);
        }
        let page_size = page_size.context("SQLite WAL has no new transaction")?;
        let checksum = self.next_checksum(&pages, commit);
        let header = Header {
            flags: HeaderFlags::empty(),
            page_size,
            commit: PageNum::new(commit)?,
            min_txid: TXID::new(self.txid + 1)?,
            max_txid: TXID::new(state.txid)?,
            timestamp: SystemTime::now(),
            pre_apply_checksum: (self.txid > 0).then_some(self.checksum),
        };
        let encoded = encode(&header, &pages, checksum)?;
        self.apply(&encoded)?;
        if self.segment_count >= 4096
            || (self.segment_count >= 128
                && self.segment_bytes >= u64::from(commit) * u64::from(page_size.into_inner()))
        {
            self.segment_count = 0;
            self.segment_bytes = 0;
            return self.checkpoint();
        }
        Ok(encoded)
    }

    pub(crate) fn apply(&mut self, bytes: &[u8]) -> Result<()> {
        let (header, pages, checksum) = decode(bytes)?;
        ensure!(
            header.max_txid.into_inner() > self.txid,
            "LTX transaction did not advance"
        );
        if header.pre_apply_checksum.is_none() {
            ensure!(
                self.txid == 0 && header.min_txid == TXID::ONE,
                "LTX snapshot cannot replace an active chain"
            );
        } else {
            ensure!(
                header.min_txid.into_inner() == self.txid + 1,
                "LTX transaction gap"
            );
            ensure!(
                header.pre_apply_checksum == Some(self.checksum),
                "LTX checksum chain mismatch"
            );
        }
        ensure!(
            self.page_size.is_none_or(|size| size == header.page_size),
            "LTX page size changed"
        );
        ensure!(
            self.next_checksum(&pages, header.commit.into_inner()) == checksum,
            "LTX database checksum mismatch"
        );
        let expected_pages = header.commit.into_inner()
            - u32::from(
                PageNum::lock_page(header.page_size).into_inner() <= header.commit.into_inner(),
            );
        if self.txid == 0 {
            ensure!(
                pages.len() == expected_pages as usize,
                "LTX snapshot is missing pages"
            );
        }
        self.write_pages(&header, &pages)?;
        for (pgno, page) in &pages {
            self.checksums
                .insert(*pgno, page.page_checksum(PageNum::new(*pgno)?));
        }
        self.checksums
            .retain(|pgno, _| *pgno <= header.commit.into_inner());
        self.checksum = checksum;
        self.txid = header.max_txid.into_inner();
        self.page_size = Some(header.page_size);
        self.segment_count += 1;
        self.segment_bytes += bytes.len() as u64;
        Ok(())
    }

    pub(crate) fn checkpoint(&self) -> Result<Vec<u8>> {
        let page_size = self.page_size.context("SQLite database has no pages")?;
        let mut file = File::open(self.path())?;
        let commit = u32::try_from(file.metadata()?.len() / u64::from(page_size.into_inner()))?;
        let mut pages = BTreeMap::new();
        for pgno in 1..=commit {
            let mut page = vec![0; page_size.into_inner() as usize];
            file.read_exact(&mut page)?;
            if pgno != PageNum::lock_page(page_size).into_inner() {
                pages.insert(pgno, page);
            }
        }
        encode(
            &Header {
                flags: HeaderFlags::empty(),
                page_size,
                commit: PageNum::new(commit)?,
                min_txid: TXID::ONE,
                max_txid: TXID::new(self.txid)?,
                timestamp: SystemTime::now(),
                pre_apply_checksum: None,
            },
            &pages,
            self.checksum,
        )
    }

    fn next_checksum(&self, pages: &BTreeMap<u32, Vec<u8>>, commit: u32) -> Checksum {
        let mut checksum = self.checksum;
        for (pgno, page) in pages {
            if let Some(old) = self.checksums.get(pgno) {
                checksum = checksum ^ *old;
            }
            checksum = checksum ^ page.page_checksum(PageNum::new(*pgno).expect("validated page"));
        }
        for (_, old) in self.checksums.range(commit.saturating_add(1)..) {
            checksum = checksum ^ *old;
        }
        checksum
    }

    fn write_pages(&self, header: &Header, pages: &BTreeMap<u32, Vec<u8>>) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.path())?;
        for (pgno, page) in pages {
            file.seek(SeekFrom::Start(
                u64::from(*pgno - 1) * u64::from(header.page_size.into_inner()),
            ))?;
            file.write_all(page)?;
        }
        file.set_len(
            u64::from(header.commit.into_inner()) * u64::from(header.page_size.into_inner()),
        )?;
        file.sync_all()?;
        Ok(())
    }
}

pub(crate) fn is_checkpoint(bytes: &[u8]) -> Result<bool> {
    let (_, header) = Decoder::new(bytes)?;
    Ok(header.pre_apply_checksum.is_none())
}

fn encode(header: &Header, pages: &BTreeMap<u32, Vec<u8>>, checksum: Checksum) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut encoder = Encoder::new(&mut bytes, header)?;
    for (pgno, page) in pages {
        encoder.encode_page(PageNum::new(*pgno)?, page)?;
    }
    encoder.finish(checksum)?;
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<(Header, BTreeMap<u32, Vec<u8>>, Checksum)> {
    let (mut decoder, header) = Decoder::new(bytes)?;
    let mut pages = BTreeMap::new();
    let mut page = vec![0; header.page_size.into_inner() as usize];
    while let Some(pgno) = decoder.decode_page(&mut page)? {
        ensure!(
            pgno.into_inner() <= header.commit.into_inner()
                && pgno != PageNum::lock_page(header.page_size),
            "invalid LTX page"
        );
        ensure!(
            pages.insert(pgno.into_inner(), page.clone()).is_none(),
            "duplicate LTX page"
        );
    }
    let trailer = decoder.finish()?;
    Ok((header, pages, trailer.post_apply_checksum))
}

#[cfg(test)]
#[path = "../tests/unit/ltx.rs"]
mod tests;
