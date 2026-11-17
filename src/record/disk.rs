//! Free space on the disk that holds the data. The recorder refuses to start below a limit
//! and stops cleanly when the space falls below it while recording: a clean stop finishes
//! every file, a full disk would leave them broken.

use std::path::Path;

/// Default limit: room for about two days of one symbol at the first live run's rate.
pub const MIN_FREE: u64 = 2_000_000_000;

/// Bytes available to this user on the file system holding `path` (or its nearest existing
/// ancestor). `None` where it cannot be told.
pub fn free_bytes(path: &Path) -> Option<u64> {
    let mut p = if path.is_relative() {
        std::env::current_dir().ok()?.join(path)
    } else {
        path.to_owned()
    };
    while !p.exists() {
        p = p.parent()?.to_owned();
    }
    statvfs_free(&p)
}

#[cfg(unix)]
fn statvfs_free(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: an all-zero statvfs is a valid value of this plain C struct.
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a NUL-terminated path and `s` a writable statvfs, as statvfs(3) needs.
    let r = unsafe { libc::statvfs(c.as_ptr(), &mut s) };
    #[allow(clippy::unnecessary_cast)] // the field types differ between platforms
    (r == 0).then(|| s.f_bavail as u64 * s.f_frsize as u64)
}

#[cfg(not(unix))]
fn statvfs_free(_path: &Path) -> Option<u64> {
    None
}

/// `123.4 GB`
pub fn gb(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / 1e9)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_space_of_a_directory_and_of_a_path_not_made_yet() {
        let dir = tempfile::tempdir().unwrap();
        let here = free_bytes(dir.path()).unwrap();
        assert!(here > 0);
        let later = free_bytes(&dir.path().join("binance/BTCUSDT/2026-11-21")).unwrap();
        // the same file system; other processes may write in between
        assert!(later.abs_diff(here) < here / 10 + 100_000_000);
        assert!(
            free_bytes(Path::new("data")).is_some(),
            "relative paths too"
        );
        assert_eq!(gb(123_456_789_012), "123.5 GB");
    }
}
