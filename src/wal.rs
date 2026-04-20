//! Append-only write-ahead log: same record layout as SST payload rows for simple replay.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let kl = u32::try_from(key.len()).map_err(|_| Error::Corrupt("key too long"))?;
        let vl = u32::try_from(value.len()).map_err(|_| Error::Corrupt("value too long"))?;
        self.file.write_all(&kl.to_le_bytes())?;
        self.file.write_all(&vl.to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Truncate WAL to empty after a successful flush.
    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        Ok(())
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
