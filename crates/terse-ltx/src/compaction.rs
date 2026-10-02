use crate::{
    Decoder, Encoder, Header, Page,
    format::{NO_CHECKSUM, require},
};
use std::io::{self, Read, Write};

/// Inputs must be ordered by transaction range. Discard output on any error.
pub fn compact<R: Read, W: Write>(readers: Vec<R>, writer: W) -> io::Result<Header> {
    require(!readers.is_empty(), "at least one LTX input required")?;
    let mut inputs = readers
        .into_iter()
        .map(Input::open)
        .collect::<io::Result<Vec<_>>>()?;
    let header = merged_header(&inputs)?;
    let mut encoder = Encoder::new(writer, header)?;
    while let Some(number) = inputs
        .iter()
        .filter_map(|input| input.page.as_ref().map(|p| p.number))
        .min()
    {
        if number <= header.commit {
            let latest = inputs
                .iter()
                .rev()
                .find_map(|input| input.page.as_ref().filter(|p| p.number == number))
                .unwrap();
            encoder.write_page(number, &latest.data)?;
        }
        for input in &mut inputs {
            if input.page.as_ref().is_some_and(|p| p.number == number) {
                input.page = input.decoder.next_page()?;
            }
        }
    }
    verify_chain(&inputs)?;
    encoder.finish(
        inputs
            .last()
            .unwrap()
            .decoder
            .trailer()
            .unwrap()
            .post_apply_checksum,
    )?;
    Ok(header)
}

struct Input<R> {
    decoder: Decoder<R>,
    page: Option<Page>,
}

impl<R: Read> Input<R> {
    fn open(reader: R) -> io::Result<Self> {
        let mut decoder = Decoder::new(reader)?;
        let page = decoder.next_page()?;
        Ok(Self { decoder, page })
    }
}

fn merged_header<R: Read>(inputs: &[Input<R>]) -> io::Result<Header> {
    for pair in inputs.windows(2) {
        let previous = pair[0].decoder.header();
        let current = pair[1].decoder.header();
        require(
            previous.page_size == current.page_size,
            "mismatched page sizes",
        )?;
        require(previous.flags == current.flags, "mismatched checksum modes")?;
        require(
            current.max_txid > previous.max_txid
                && current.min_txid <= previous.max_txid.saturating_add(1),
            "noncontiguous transaction ranges",
        )?;
    }
    let first = inputs[0].decoder.header();
    let last = inputs.last().unwrap().decoder.header();
    Ok(Header {
        flags: first.flags,
        page_size: first.page_size,
        commit: last.commit,
        min_txid: first.min_txid,
        max_txid: last.max_txid,
        timestamp: last.timestamp,
        pre_apply_checksum: first.pre_apply_checksum,
        ..Header::default()
    })
}

fn verify_chain<R: Read>(inputs: &[Input<R>]) -> io::Result<()> {
    for pair in inputs.windows(2) {
        let previous = &pair[0].decoder;
        let next = pair[1].decoder.header();
        if next.flags & NO_CHECKSUM == 0
            && previous.header().max_txid.checked_add(1) == Some(next.min_txid)
        {
            require(
                previous.trailer().unwrap().post_apply_checksum == next.pre_apply_checksum,
                "transaction checksum mismatch",
            )?;
        }
    }
    Ok(())
}
