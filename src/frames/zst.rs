//! Compressed frames files: `<name>.frames.zst` holds exactly the bytes of `<name>.frames`
//! as one zstd frame with its content checksum.
//!
//! The recorder compresses an hourly file once it is finished: it writes the compressed
//! copy as `.tmp`, decompresses it again and compares every byte with the original, and
//! only then renames it and removes the original. Every reader of frames opens both forms
//! (`open`), so nothing downstream needs to know.

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};

/// The first bytes of every zstd frame.
pub const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Compression level of the recorder and of `frames compress`: most of the ratio of the
/// high levels at a fraction of their time.
pub const LEVEL: i32 = 9;

/// Open a frames file in either form for reading.
pub fn open(path: &Path) -> io::Result<Box<dyn BufRead + Send>> {
    let mut raw = BufReader::with_capacity(1 << 20, File::open(path)?);
    if raw.fill_buf()?.starts_with(&ZSTD_MAGIC) {
        let decoder = zstd::stream::read::Decoder::with_buffer(raw)?;
        Ok(Box::new(BufReader::with_capacity(1 << 20, decoder)))
    } else {
        Ok(Box::new(raw))
    }
}

/// The compressed name of a frames file: `<name>.frames.zst`.
pub fn compressed_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".zst");
    PathBuf::from(p)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compressed {
    pub path: PathBuf,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

/// Compress a finished frames file next to itself and remove the original once the
/// compressed copy has been read back and found identical. On any error the original
/// stays and no compressed file is left behind.
pub fn compress_file(path: &Path, level: i32) -> io::Result<Compressed> {
    let target = compressed_path(path);
    if target.exists() {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!("{} already exists", target.display()),
        ));
    }
    let tmp = {
        let mut t = target.as_os_str().to_owned();
        t.push(".tmp");
        PathBuf::from(t)
    };
    let result = (|| {
        let out = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        let mut enc = zstd::stream::write::Encoder::new(out, level)?;
        enc.include_checksum(true)?;
        let mut src = BufReader::with_capacity(1 << 20, File::open(path)?);
        let raw_bytes = io::copy(&mut src, &mut enc)?;
        let out = enc.finish()?;
        out.sync_all()?;
        drop(out);
        if !same_content(path, &tmp)? {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "the compressed copy does not read back as the original",
            ));
        }
        Ok(raw_bytes)
    })();
    let raw_bytes = match result {
        Ok(n) => n,
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
    };
    fs::rename(&tmp, &target)?;
    sync_dir(&target)?;
    fs::remove_file(path)?;
    sync_dir(path)?;
    Ok(Compressed {
        compressed_bytes: fs::metadata(&target)?.len(),
        path: target,
        raw_bytes,
    })
}

/// Whether the zstd file `zst` decompresses to exactly the bytes of `raw`.
fn same_content(raw: &Path, zst: &Path) -> io::Result<bool> {
    let mut a = BufReader::with_capacity(1 << 20, File::open(raw)?);
    let mut b = zstd::stream::read::Decoder::new(File::open(zst)?)?;
    let (mut x, mut y) = (vec![0u8; 1 << 16], vec![0u8; 1 << 16]);
    loop {
        let n = a.read(&mut x)?;
        if n == 0 {
            return Ok(b.read(&mut y[..1])? == 0);
        }
        b.read_exact(&mut y[..n])?;
        if x[..n] != y[..n] {
            return Ok(false);
        }
    }
}

fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::tests::header;
    use crate::frames::{FrameKind, FramesFile, FramesReader};

    fn frames_file(dir: &Path) -> PathBuf {
        let path = dir.join("13-00-00.frames");
        let mut w = FramesFile::create(&path, &header()).unwrap();
        for i in 0..2000 {
            let payload = format!(
                r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{},"s":"BTCUSDT","b":"97123.{:02}000000","B":"0.{:05}000","a":"97123.{:02}000000","A":"0.50000000"}}}}"#,
                1000 + i,
                i % 100,
                i * 7 % 100_000,
                (i + 1) % 100
            );
            w.write_frame(crate::frames::Frame {
                kind: FrameKind::Text,
                recv_ts: 1_795_219_200_000_000_000 + i * 1_000_000,
                aux: 1,
                payload: payload.as_bytes(),
            })
            .unwrap();
        }
        w.finish().unwrap()
    }

    fn read_all(r: impl Read) -> Vec<u8> {
        let mut v = Vec::new();
        BufReader::new(r).read_to_end(&mut v).unwrap();
        v
    }

    #[test]
    fn compressed_file_reads_back_as_the_original_and_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = frames_file(dir.path());
        let original = fs::read(&path).unwrap();
        let c = compress_file(&path, LEVEL).unwrap();
        assert_eq!(c.path, dir.path().join("13-00-00.frames.zst"));
        assert!(!path.exists(), "the original is removed");
        assert_eq!(c.raw_bytes, original.len() as u64);
        assert!(
            c.compressed_bytes * 5 < c.raw_bytes,
            "{} -> {}",
            c.raw_bytes,
            c.compressed_bytes
        );
        assert_eq!(read_all(open(&c.path).unwrap()), original);
        // and the frames reader takes it through `open`
        let mut r = FramesReader::new(open(&c.path).unwrap()).unwrap();
        let mut n = 0;
        while r.next_frame().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 2000);
        assert!(
            fs::read_dir(dir.path()).unwrap().count() == 1,
            "no temporary file left"
        );
    }

    #[test]
    fn an_existing_target_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = frames_file(dir.path());
        fs::write(compressed_path(&path), b"older").unwrap();
        let err = compress_file(&path, LEVEL).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
        assert!(path.exists());
        assert_eq!(fs::read(compressed_path(&path)).unwrap(), b"older");
    }

    #[test]
    fn a_damaged_compressed_file_is_an_error_not_wrong_data() {
        let dir = tempfile::tempdir().unwrap();
        let c = compress_file(&frames_file(dir.path()), LEVEL).unwrap();
        let mut bytes = fs::read(&c.path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x10;
        fs::write(&c.path, &bytes).unwrap();
        let mut out = Vec::new();
        let r = open(&c.path).and_then(|mut r| r.read_to_end(&mut out));
        assert!(r.is_err(), "the content checksum catches it");
    }

    #[test]
    fn a_missing_original_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gone.frames");
        assert!(compress_file(&path, LEVEL).is_err());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
