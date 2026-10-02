use std::{fs, io, path::PathBuf, process::Command};
use terse_ltx::{Decoder, Header, Page, compact};

#[test]
fn compacts_go_history_with_updates_and_truncation() -> io::Result<()> {
    for flags in [0, 2] {
        for first in ["1", "frame"] {
            let history = history(flags, first);
            let mut output = Vec::new();
            let header = compact(history.iter().map(Vec::as_slice).collect(), &mut output)?;
            let expected = decode(&fixture(flags, "merged"))?;
            assert_eq!(header, expected.0);
            assert_eq!(decode(&output)?, expected);
            assert_eq!(header.min_txid, 1);
            assert_eq!(header.max_txid, 3);
            assert_eq!(header.commit, 3);
        }
    }
    Ok(())
}

#[test]
fn rejects_corruption_truncation_and_transaction_gaps() {
    for first in ["1", "frame"] {
        let valid = fixture(0, first);
        for length in 0..valid.len() {
            assert!(
                compact(vec![&valid[..length]], Vec::new()).is_err(),
                "accepted truncated file at {length}"
            );
        }
        for offset in [0, 4, 8, 16, 40, 100, 106, valid.len() - 25, valid.len() - 1] {
            let mut damaged = valid.clone();
            damaged[offset] ^= 0x40;
            assert!(
                compact(vec![damaged.as_slice()], Vec::new()).is_err(),
                "accepted corruption at {offset}"
            );
        }
    }
    let history = history(2, "1");
    for indices in [vec![0, 2], vec![1, 0], vec![0, 0], vec![]] {
        assert!(
            compact(
                indices.iter().map(|&i| history[i].as_slice()).collect(),
                Vec::new()
            )
            .is_err()
        );
    }
}

#[test]
#[ignore = "requires Go to verify Rust output with superfly/ltx v0.5.2"]
fn go_restores_rust_compaction() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let oracle = directory.path().join("oracle");
    let status = Command::new("go")
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oracle"))
        .args(["build", "-mod=readonly", "-o"])
        .arg(&oracle)
        .arg(".")
        .status()?;
    assert!(status.success());
    for flags in [0, 2] {
        for first in ["1", "frame"] {
            let history = history(flags, first);
            let mut output = Vec::new();
            compact(history.iter().map(Vec::as_slice).collect(), &mut output)?;
            let path = directory.path().join("rust.ltx");
            fs::write(&path, output)?;
            let result = Command::new(&oracle)
                .arg("verify")
                .arg(path)
                .arg(fixture_path(flags, "merged"))
                .output()?;
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }
    Ok(())
}

fn history(flags: u32, first: &str) -> Vec<Vec<u8>> {
    [first, "2", "3"].map(|name| fixture(flags, name)).into()
}

fn fixture(flags: u32, name: &str) -> Vec<u8> {
    fs::read(fixture_path(flags, name)).unwrap()
}

fn fixture_path(flags: u32, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/{flags}-{name}.ltx"))
}

fn decode(bytes: &[u8]) -> io::Result<(Header, Vec<Page>)> {
    let mut reader = Decoder::new(bytes)?;
    let header = *reader.header();
    let mut pages = Vec::new();
    while let Some(page) = reader.next_page()? {
        pages.push(page);
    }
    Ok((header, pages))
}
