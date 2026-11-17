//! Frames files rotated by the UTC hour of the frames they hold.
//!
//! `<root>/<date>/frames/<HH-MM-SS>.frames`, named after the receive time of the first frame
//! in the file. A new file starts at each UTC hour boundary, so a day directory holds the
//! frames of that day only. Every file is complete on its own: it starts with the header and
//! is finished (synced and renamed from `.tmp`) when the hour ends or recording stops.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::frames::{FramesFile, Header, OwnedFrame};
use crate::time::{Utc, hour_start};

pub struct Rotator {
    root: PathBuf,
    header: Header,
    current: Option<(i64, FramesFile, PathBuf)>,
    finished: Vec<PathBuf>,
    bytes: u64,
}

impl Rotator {
    /// `root` is the symbol directory, for example `data/binance/BTCUSDT`.
    pub fn new(root: &Path, header: Header) -> Self {
        Self {
            root: root.to_owned(),
            header,
            current: None,
            finished: Vec::new(),
            bytes: 0,
        }
    }

    pub fn write(&mut self, frame: &OwnedFrame) -> io::Result<()> {
        let hour = hour_start(frame.recv_ts);
        if self.current.as_ref().is_none_or(|(h, _, _)| *h != hour) {
            self.close_current()?;
            let t = Utc::from_ns(frame.recv_ts);
            let dir = self.root.join(t.date()).join("frames");
            fs::create_dir_all(&dir)?;
            let path = dir.join(format!("{}.frames", t.time()));
            let file = FramesFile::create(&path, &self.header)?;
            self.current = Some((hour, file, path));
        }
        let (_, file, _) = self.current.as_mut().expect("opened above");
        file.write_frame(frame.as_frame())?;
        self.bytes += 25 + frame.payload.len() as u64;
        Ok(())
    }

    /// Bytes of frame records written so far.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The file being written, if any.
    pub fn current_path(&self) -> Option<&Path> {
        self.current.as_ref().map(|(_, _, p)| p.as_path())
    }

    /// Finish the open file and return every finished file in order.
    pub fn finish(mut self) -> io::Result<Vec<PathBuf>> {
        self.close_current()?;
        Ok(self.finished)
    }

    fn close_current(&mut self) -> io::Result<()> {
        if let Some((_, file, _)) = self.current.take() {
            self.finished.push(file.finish()?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::{FrameKind, FramesReader};
    use crate::json::TimeUnit;
    use crate::time::{NS_PER_HOUR, NS_PER_SEC};

    fn header() -> Header {
        Header {
            symbol: "BTCUSDT".into(),
            price_step: "0.01000000".into(),
            qty_step: "0.00001000".into(),
            time_unit: TimeUnit::Micro,
            recorder: "test".into(),
        }
    }

    fn frame(ts: i64, n: u8) -> OwnedFrame {
        OwnedFrame {
            kind: FrameKind::Text,
            recv_ts: ts,
            aux: 1,
            payload: vec![n],
        }
    }

    fn read(path: &Path) -> Vec<OwnedFrame> {
        let mut r = FramesReader::new(fs::File::open(path).unwrap()).unwrap();
        let mut out = Vec::new();
        while let Some(f) = r.next_frame().unwrap() {
            out.push(f.to_owned());
        }
        out
    }

    #[test]
    fn rotates_at_utc_hours_and_days() {
        let dir = tempfile::tempdir().unwrap();
        let day = 1_795_219_200 * NS_PER_SEC; // 2026-11-21 00:00 UTC
        let t1 = day + 23 * NS_PER_HOUR + 59 * 60 * NS_PER_SEC + 58 * NS_PER_SEC; // 23:59:58
        let frames = [
            frame(t1, 1),
            frame(t1 + NS_PER_SEC, 2),
            frame(t1 + 3 * NS_PER_SEC, 3),
            frame(t1 + NS_PER_HOUR + 3 * NS_PER_SEC, 4), // 01:00:01
        ];
        let mut w = Rotator::new(dir.path(), header());
        for f in &frames {
            w.write(f).unwrap();
        }
        assert!(
            w.current_path()
                .unwrap()
                .ends_with("2026-11-22/frames/01-00-01.frames")
        );
        let files = w.finish().unwrap();
        let rel: Vec<String> = files
            .iter()
            .map(|p| p.strip_prefix(dir.path()).unwrap().display().to_string())
            .collect();
        assert_eq!(
            rel,
            [
                "2026-11-21/frames/23-59-58.frames",
                "2026-11-22/frames/00-00-01.frames",
                "2026-11-22/frames/01-00-01.frames"
            ]
        );
        assert_eq!(read(&files[0]), frames[..2]);
        assert_eq!(read(&files[1]), frames[2..3]);
        assert_eq!(read(&files[2]), frames[3..]);
        assert!(
            !files
                .iter()
                .any(|p| p.with_extension("frames.tmp").exists())
        );
    }

    #[test]
    fn nothing_written_means_no_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            Rotator::new(dir.path(), header())
                .finish()
                .unwrap()
                .is_empty()
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
