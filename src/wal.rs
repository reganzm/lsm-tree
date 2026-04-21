//! Append-only write-ahead log: same record layout as SST payload rows for simple replay.
//!
//! **组提交**：`append_put` 只 `write` 到内核缓冲区，不在每条记录后 `fsync`（避免 Windows 上
//! 百万次 `fsync` 触发杀毒/锁竞争导致 `PermissionDenied`）。在 [`Wal::sync`]（memtable flush
//! 前）和 [`Wal::reset`] 时再落盘 / 截断。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::io_retry::{sync_all_retry_patient, truncate_and_sync_retry, write_all_retry};

pub struct Wal {
    path: PathBuf,
    file: File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self { path, file })
    }

    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let kl = u32::try_from(key.len()).map_err(|_| Error::Corrupt("key too long"))?;
        let vl = u32::try_from(value.len()).map_err(|_| Error::Corrupt("value too long"))?;
        let mut buf = Vec::with_capacity(8 + key.len() + value.len());
        buf.extend_from_slice(&kl.to_le_bytes());
        buf.extend_from_slice(&vl.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        write_all_retry(&mut self.file, &buf)?;
        Ok(())
    }

    /// 将缓冲区中的 WAL 记录刷盘。在 memtable flush 前调用（长时间 patience，适配 Windows）。
    pub fn sync(&mut self) -> Result<()> {
        sync_all_retry_patient(&self.file)?;
        Ok(())
    }

    /// Truncate WAL to empty after a successful flush.
    pub fn reset(&mut self) -> Result<()> {
        match truncate_and_sync_retry(&self.file) {
            Ok(()) => Ok(()),
            Err(e)
                if e.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                // 关闭旧句柄并重建空文件，避免 Defender 长时间占用导致 truncate 永久失败。
                self.file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(&self.path)?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn replay(&mut self, mut on_put: impl FnMut(Vec<u8>, Vec<u8>)) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut off = 0usize;
        while off + 8 <= buf.len() {
            let kl = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let vl = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()) as usize;
            off += 8;
            if off + kl + vl > buf.len() {
                return Err(Error::Corrupt("truncated wal record"));
            }
            let key = buf[off..off + kl].to_vec();
            let val = buf[off + kl..off + kl + vl].to_vec();
            off += kl + vl;
            on_put(key, val);
        }
        if off != buf.len() {
            return Err(Error::Corrupt("trailing wal bytes"));
        }
        Ok(())
    }
}
