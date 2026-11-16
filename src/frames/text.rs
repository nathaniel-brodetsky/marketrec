//! Text form of a frames file: JSON Lines, one object per line, readable and diffable.
//!
//! The first line is `{"header":{...}}`, every following line one record:
//!
//! ```text
//! {"kind":"text","recv_ts":1795219200000000000,"aux":1,"payload_json":{"stream":"btcusdt@trade","data":{..}}}
//! {"kind":"connected","recv_ts":1795219200000100000,"aux":1,"payload":"wss://stream.binance.com:9443/stream"}
//! {"kind":"binary","recv_ts":1795219200000200000,"aux":2,"payload_hex":"00ff01"}
//! {"kind":"disconnected","recv_ts":1795219200000300000,"aux":1}
//! ```
//!
//! The payload is written in the most readable form that keeps its exact bytes:
//! - `payload_json`: a JSON object or array on one line, embedded verbatim (not re-encoded);
//! - `payload`: any other UTF-8 text, as a JSON string;
//! - `payload_hex`: anything else;
//! - nothing: an empty payload.
//!
//! Conversion in both directions is lossless. Test fixtures are kept in this form so that a
//! reviewer can read every scenario.

use std::fmt;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{FrameKind, FramesError, FramesFile, FramesReader, FramesWriter, Header, OwnedFrame};

#[derive(Serialize, Deserialize)]
struct HeaderLine {
    header: Header,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    kind: FrameKind,
    recv_ts: i64,
    aux: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_json: Option<Box<RawValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_hex: Option<String>,
}

#[derive(Debug)]
pub enum TextError {
    Io(io::Error),
    Frames(FramesError),
    /// `line` is 1-based.
    Line {
        line: usize,
        msg: String,
    },
}

impl fmt::Display for TextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TextError::Io(e) => write!(f, "i/o: {e}"),
            TextError::Frames(e) => write!(f, "{e}"),
            TextError::Line { line, msg } => write!(f, "line {line}: {msg}"),
        }
    }
}

impl std::error::Error for TextError {}

impl From<io::Error> for TextError {
    fn from(e: io::Error) -> Self {
        TextError::Io(e)
    }
}

impl From<FramesError> for TextError {
    fn from(e: FramesError) -> Self {
        TextError::Frames(e)
    }
}

/// Binary frames to JSON Lines. Returns the number of records. If the input is damaged,
/// everything before the damage is written and the error is returned.
pub fn dump(input: impl Read, out: &mut impl Write) -> Result<usize, TextError> {
    let mut reader = FramesReader::new(input)?;
    serde_json::to_writer(
        &mut *out,
        &HeaderLine {
            header: reader.header().clone(),
        },
    )
    .map_err(io::Error::from)?;
    out.write_all(b"\n")?;
    let mut n = 0;
    while let Some(f) = reader.next_frame()? {
        let (mut payload, mut payload_json, mut payload_hex) = (None, None, None);
        if !f.payload.is_empty() {
            match std::str::from_utf8(f.payload) {
                Ok(s) => match embeddable_json(s) {
                    Some(raw) => payload_json = Some(raw),
                    None => payload = Some(s.to_owned()),
                },
                Err(_) => payload_hex = Some(to_hex(f.payload)),
            }
        }
        let rec = Record {
            kind: f.kind,
            recv_ts: f.recv_ts,
            aux: f.aux,
            payload,
            payload_json,
            payload_hex,
        };
        serde_json::to_writer(&mut *out, &rec).map_err(io::Error::from)?;
        out.write_all(b"\n")?;
        n += 1;
    }
    Ok(n)
}

/// JSON Lines to binary frames in any byte sink. Returns the number of records.
pub fn pack(input: impl BufRead, out: impl Write) -> Result<usize, TextError> {
    let mut lines = input.lines().enumerate();
    let header = read_header(&mut lines)?;
    let mut writer = FramesWriter::new(out, &header)?;
    let n = for_each_record(lines, |f| writer.write_frame(f.as_frame()))?;
    writer.flush()?;
    Ok(n)
}

/// JSON Lines to a frames file, written as `<path>.tmp` and renamed when complete.
pub fn pack_file(input: impl BufRead, path: &Path) -> Result<usize, TextError> {
    let mut lines = input.lines().enumerate();
    let header = read_header(&mut lines)?;
    let mut file = FramesFile::create(path, &header)?;
    let n = for_each_record(lines, |f| file.write_frame(f.as_frame()))?;
    file.finish()?;
    Ok(n)
}

type Lines<B> = std::iter::Enumerate<io::Lines<B>>;

fn read_header<B: BufRead>(lines: &mut Lines<B>) -> Result<Header, TextError> {
    loop {
        let Some((i, line)) = lines.next() else {
            return Err(TextError::Line {
                line: 1,
                msg: "missing header line".into(),
            });
        };
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let h: HeaderLine = serde_json::from_str(&line).map_err(|e| TextError::Line {
            line: i + 1,
            msg: e.to_string(),
        })?;
        return Ok(h.header);
    }
}

fn for_each_record<B: BufRead>(
    lines: Lines<B>,
    mut write: impl FnMut(OwnedFrame) -> io::Result<()>,
) -> Result<usize, TextError> {
    let mut n = 0;
    for (i, line) in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        write(parse_record(&line).map_err(|msg| TextError::Line { line: i + 1, msg })?)?;
        n += 1;
    }
    Ok(n)
}

/// Streams the records of a JSON Lines file.
pub struct TextReader<B: BufRead> {
    lines: Lines<B>,
    header: Header,
}

impl<B: BufRead> TextReader<B> {
    pub fn new(input: B) -> Result<Self, TextError> {
        let mut lines = input.lines().enumerate();
        let header = read_header(&mut lines)?;
        Ok(Self { lines, header })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn next_frame(&mut self) -> Result<Option<OwnedFrame>, TextError> {
        for (i, line) in self.lines.by_ref() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            return parse_record(&line)
                .map(Some)
                .map_err(|msg| TextError::Line { line: i + 1, msg });
        }
        Ok(None)
    }
}

/// Read all records of a JSON Lines file, for tests and tools.
pub fn read_all(input: impl BufRead) -> Result<(Header, Vec<OwnedFrame>), TextError> {
    let mut bytes = Vec::new();
    pack(input, &mut bytes)?;
    let mut r = FramesReader::new(&bytes[..])?;
    let header = r.header().clone();
    let mut frames = Vec::new();
    while let Some(f) = r.next_frame()? {
        frames.push(f.to_owned());
    }
    Ok((header, frames))
}

fn parse_record(line: &str) -> Result<OwnedFrame, String> {
    let rec: Record = serde_json::from_str(line).map_err(|e| e.to_string())?;
    let payload = match (rec.payload, rec.payload_json, rec.payload_hex) {
        (Some(s), None, None) => s.into_bytes(),
        (None, Some(j), None) => j.get().as_bytes().to_vec(),
        (None, None, Some(h)) => from_hex(&h).ok_or("payload_hex is not valid hex")?,
        (None, None, None) => Vec::new(),
        _ => return Err("more than one of payload, payload_json, payload_hex".into()),
    };
    Ok(OwnedFrame {
        kind: rec.kind,
        recv_ts: rec.recv_ts,
        aux: rec.aux,
        payload,
    })
}

/// A payload that can be embedded as JSON without changing a byte: an object or array on
/// one line, without surrounding whitespace.
fn embeddable_json(s: &str) -> Option<Box<RawValue>> {
    let shaped =
        (s.starts_with('{') || s.starts_with('[')) && s.trim() == s && !s.contains(['\n', '\r']);
    if !shaped {
        return None;
    }
    RawValue::from_string(s.to_owned())
        .ok()
        .filter(|raw| raw.get() == s)
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 15)] as char);
    }
    s
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.as_bytes();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    s.chunks(2)
        .map(|p| Some(nibble(p[0])? << 4 | nibble(p[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::frames::tests::header;

    const TEXT: &str = concat!(
        r#"{"header":{"symbol":"BTCUSDT","price_step":"0.01000000","qty_step":"0.00001000","time_unit":"micro","recorder":"marketrec test"}}"#,
        "\n",
        r#"{"kind":"connected","recv_ts":10,"aux":1,"payload":"wss://example/stream"}"#,
        "\n",
        r#"{"kind":"text","recv_ts":11,"aux":1,"payload_json":{"stream":"btcusdt@trade","data":{"p": [1, 2.50]}}}"#,
        "\n",
        r#"{"kind":"binary","recv_ts":12,"aux":2,"payload_hex":"00ff01"}"#,
        "\n",
        r#"{"kind":"disconnected","recv_ts":13,"aux":1}"#,
        "\n",
    );

    #[test]
    fn text_to_binary_and_back_is_identical() {
        let mut bin = Vec::new();
        assert_eq!(pack(TEXT.as_bytes(), &mut bin).unwrap(), 4);
        let mut text = Vec::new();
        assert_eq!(dump(&bin[..], &mut text).unwrap(), 4);
        assert_eq!(String::from_utf8(text).unwrap(), TEXT);
    }

    #[test]
    fn read_all_gives_header_and_frames() {
        let (h, frames) = read_all(TEXT.as_bytes()).unwrap();
        assert_eq!(h, header());
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[2].payload, vec![0, 255, 1]);
        assert!(frames[3].payload.is_empty());
    }

    #[test]
    fn errors_name_the_line() {
        let bad = TEXT.replace(r#""kind":"binary""#, r#""kind":"video""#);
        assert!(matches!(
            pack(bad.as_bytes(), Vec::new()),
            Err(TextError::Line { line: 4, .. })
        ));
        let both = TEXT.replace(
            r#""payload_hex":"00ff01""#,
            r#""payload":"x","payload_hex":"00""#,
        );
        assert!(matches!(
            pack(both.as_bytes(), Vec::new()),
            Err(TextError::Line { line: 4, .. })
        ));
        let odd = TEXT.replace("00ff01", "0ff01");
        assert!(matches!(
            pack(odd.as_bytes(), Vec::new()),
            Err(TextError::Line { line: 4, .. })
        ));
        let extra = TEXT.replace(r#""aux":2,"#, r#""aux":2,"note":1,"#);
        assert!(matches!(
            pack(extra.as_bytes(), Vec::new()),
            Err(TextError::Line { line: 4, .. })
        ));
        assert!(matches!(
            pack(&b""[..], Vec::new()),
            Err(TextError::Line { line: 1, .. })
        ));
    }

    #[test]
    fn dump_writes_everything_before_damage() {
        let mut bin = Vec::new();
        pack(TEXT.as_bytes(), &mut bin).unwrap();
        bin.truncate(bin.len() - 2);
        let mut text = Vec::new();
        assert!(matches!(
            dump(&bin[..], &mut text),
            Err(TextError::Frames(FramesError::Truncated { .. }))
        ));
        assert_eq!(
            String::from_utf8(text).unwrap().lines().count(),
            4,
            "header and three intact records"
        );
    }

    #[test]
    fn pack_file_writes_through_a_temporary_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.frames");
        assert_eq!(pack_file(TEXT.as_bytes(), &path).unwrap(), 4);
        assert!(!dir.path().join("x.frames.tmp").exists());
        let mut text = Vec::new();
        dump(std::fs::File::open(&path).unwrap(), &mut text).unwrap();
        assert_eq!(String::from_utf8(text).unwrap(), TEXT);
    }

    #[test]
    fn payload_forms() {
        assert!(
            embeddable_json(r#"{"a": [1, 2]}"#).is_some(),
            "inner whitespace is kept verbatim"
        );
        assert!(embeddable_json("[1,2]").is_some());
        for not_embedded in [
            r#""a string""#,
            "123",
            " {}",
            "{} ",
            "{\n}",
            "{broken",
            "plain text",
        ] {
            assert!(embeddable_json(not_embedded).is_none(), "{not_embedded:?}");
        }
        let two = TEXT.replace(
            r#""payload":"wss://example/stream""#,
            r#""payload":"x","payload_json":{}"#,
        );
        assert!(matches!(
            pack(two.as_bytes(), Vec::new()),
            Err(TextError::Line { line: 2, .. })
        ));
    }

    proptest! {
        #[test]
        fn hex_roundtrip(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            prop_assert_eq!(from_hex(&to_hex(&bytes)), Some(bytes));
        }

        #[test]
        fn json_like_payloads_roundtrip(body in r#"[\[{][ -~]{0,30}"#) {
            let mut w = FramesWriter::new(Vec::new(), &header()).unwrap();
            w.write_frame(crate::frames::Frame { kind: FrameKind::Text, recv_ts: 1, aux: 1, payload: body.as_bytes() }).unwrap();
            let bin = w.into_inner();
            let mut text = Vec::new();
            dump(&bin[..], &mut text).unwrap();
            let mut again = Vec::new();
            pack(&text[..], &mut again).unwrap();
            prop_assert_eq!(again, bin);
        }

        #[test]
        fn binary_to_text_and_back(payloads in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..32), 0..10)) {
            let mut w = FramesWriter::new(Vec::new(), &header()).unwrap();
            for (i, p) in payloads.iter().enumerate() {
                w.write_frame(crate::frames::Frame { kind: FrameKind::Binary, recv_ts: i as i64, aux: 0, payload: p }).unwrap();
            }
            let bin = w.into_inner();
            let mut text = Vec::new();
            dump(&bin[..], &mut text).unwrap();
            let mut again = Vec::new();
            pack(&text[..], &mut again).unwrap();
            prop_assert_eq!(again, bin);
        }
    }
}
