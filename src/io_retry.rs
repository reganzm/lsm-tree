//! Windows 等环境下，杀毒/索引器可能造成短暂的 `PermissionDenied`，对 sync / 删文件做有限重试。

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(windows)]
const SYNC_ATTEMPTS: u32 = 96;
#[cfg(not(windows))]
const SYNC_ATTEMPTS: u32 = 24;

const REMOVE_ATTEMPTS: u32 = 24;

/// 对 `sync_all` 做重试（单次「爆发」尝试）。
pub fn sync_all_retry(file: &File) -> std::io::Result<()> {
    for attempt in 0..SYNC_ATTEMPTS {
        match file.sync_all() {
            Ok(()) => return Ok(()),
            Err(e)
                if matches!(e.kind(), ErrorKind::PermissionDenied | ErrorKind::Interrupted)
                    && attempt + 1 < SYNC_ATTEMPTS =>
            {
                #[cfg(windows)]
                let ms = 40u64 + 15 * u64::from(attempt);
                #[cfg(not(windows))]
                let ms = 8u64 + 12 * u64::from(attempt);
                thread::sleep(Duration::from_millis(ms));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// 在较长时间内反复调用 [`sync_all_retry`]，用于 memtable flush 等关键路径。
pub fn sync_all_retry_patient(file: &File) -> std::io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match sync_all_retry(file) {
            Ok(()) => return Ok(()),
            Err(e)
                if matches!(e.kind(), ErrorKind::PermissionDenied | ErrorKind::Interrupted)
                    && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(120));
            }
            Err(e) => return Err(e),
        }
    }
}

pub fn truncate_and_sync_retry(file: &File) -> std::io::Result<()> {
    for attempt in 0..SYNC_ATTEMPTS {
        match file.set_len(0) {
            Err(e)
                if matches!(e.kind(), ErrorKind::PermissionDenied | ErrorKind::Interrupted)
                    && attempt + 1 < SYNC_ATTEMPTS =>
            {
                #[cfg(windows)]
                let ms = 40u64 + 15 * u64::from(attempt);
                #[cfg(not(windows))]
                let ms = 8u64 + 12 * u64::from(attempt);
                thread::sleep(Duration::from_millis(ms));
            }
            Err(e) => return Err(e),
            Ok(()) => return sync_all_retry(file),
        }
    }
    unreachable!()
}

pub fn remove_file_retry(path: impl AsRef<Path>) -> std::io::Result<()> {
    let path = path.as_ref();
    for attempt in 0..REMOVE_ATTEMPTS {
        match fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e)
                if matches!(e.kind(), ErrorKind::PermissionDenied | ErrorKind::Interrupted)
                    && attempt + 1 < REMOVE_ATTEMPTS =>
            {
                thread::sleep(Duration::from_millis(10 + 15 * u64::from(attempt)));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// `write` 遇 `PermissionDenied` 时重试剩余缓冲区（与 `std::io::Write::write_all` 语义一致）。
pub fn write_all_retry(mut w: impl Write, mut buf: &[u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        match w.write(buf) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(n) => buf = &buf[n..],
            Err(e)
                if matches!(e.kind(), ErrorKind::PermissionDenied | ErrorKind::Interrupted) =>
            {
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
