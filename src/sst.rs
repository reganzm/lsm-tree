//! On-disk sorted string table: payload + bloom + sparse index + footer.

use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::bloom::Bloom;
use crate::error::{Error, Result};
use crate::io_retry::sync_all_retry;

const MAGIC: u32 = 0x4C53_5354; // 'LSST'
const INDEX_EVERY: usize = 32;
const FOOTER_SIZE: usize = 20;

#[derive(Clone, Debug)]
pub struct SstMeta {
    pub path: PathBuf,
    pub id: u64,
    pub seq: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub size_bytes: u64,
}

pub struct SstWriter {
    file: File,
    path: PathBuf,
    bytes_written: u64,
    index: Vec<(Vec<u8>, u64)>,
    pending_since_index: usize,
    first_key: Option<Vec<u8>>,
    last_key: Vec<u8>,
    bloom: Bloom,
}

impl SstWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            bytes_written: 0,
            index: Vec::new(),
            pending_since_index: 0,
            first_key: None,
            last_key: Vec::new(),
            bloom: Bloom::new(),
        })
    }

    pub fn write_kv(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = key.to_vec();
        self.bloom.add(key);

        let offset = self.bytes_written;
        if self.index.is_empty() {
            self.index.push((key.to_vec(), 0));
        } else if self.pending_since_index >= INDEX_EVERY {
            self.index.push((key.to_vec(), offset));
            self.pending_since_index = 0;
        }
        self.pending_since_index += 1;

        let kl = u32::try_from(key.len()).map_err(|_| Error::Corrupt("key too long"))?;
        let vl = u32::try_from(value.len()).map_err(|_| Error::Corrupt("value too long"))?;
        self.file.write_all(&kl.to_le_bytes())?;
        self.file.write_all(&vl.to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        let row = 8 + key.len() + value.len();
        self.bytes_written += row as u64;
        Ok(())
    }

    pub fn finish(mut self, id: u64, seq: u64) -> Result<SstMeta> {
        let data_end = self.bytes_written;
        let min_key = self.first_key.ok_or(Error::Corrupt("empty sst"))?;
        let max_key = self.last_key.clone();

        let bloom_bytes = self.bloom.to_bytes();
        let bloom_len = u32::try_from(bloom_bytes.len()).map_err(|_| Error::Corrupt("bloom too large"))?;
        self.file.write_all(&bloom_bytes)?;

        let mut index_buf: Vec<u8> = Vec::new();
        let n = u32::try_from(self.index.len()).map_err(|_| Error::Corrupt("index too large"))?;
        index_buf.extend_from_slice(&n.to_le_bytes());
        for (k, off) in &self.index {
            let kl = u32::try_from(k.len()).map_err(|_| Error::Corrupt("key too long"))?;
            index_buf.extend_from_slice(&kl.to_le_bytes());
            index_buf.extend_from_slice(k);
            index_buf.extend_from_slice(&off.to_le_bytes());
        }
        let index_len = u32::try_from(index_buf.len()).map_err(|_| Error::Corrupt("index too large"))?;
        self.file.write_all(&index_buf)?;

        self.file.write_all(&data_end.to_le_bytes())?;
        self.file.write_all(&bloom_len.to_le_bytes())?;
        self.file.write_all(&index_len.to_le_bytes())?;
        self.file.write_all(&MAGIC.to_le_bytes())?;
        sync_all_retry(&self.file)?;

        let size = self.file.metadata()?.len();
        Ok(SstMeta {
            path: self.path,
            id,
            seq,
            min_key,
            max_key,
            size_bytes: size,
        })
    }
}

pub struct SstReader {
    pub meta: SstMeta,
    data: Vec<u8>,
    data_end: u64,
    index: Vec<(Vec<u8>, u64)>,
    bloom: Bloom,
}

impl SstReader {
    pub fn open(meta: SstMeta) -> Result<Self> {
        let mut file = File::open(&meta.path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        if raw.len() < FOOTER_SIZE {
            return Err(Error::Corrupt("sst too small"));
        }
        let base = raw.len() - FOOTER_SIZE;
        let data_end = u64::from_le_bytes(raw[base..base + 8].try_into().unwrap());
        let bloom_len = u32::from_le_bytes(raw[base + 8..base + 12].try_into().unwrap()) as usize;
        let index_len = u32::from_le_bytes(raw[base + 12..base + 16].try_into().unwrap()) as usize;
        let magic = u32::from_le_bytes(raw[base + 16..base + 20].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::Corrupt("bad sst magic"));
        }

        let index_start = base.checked_sub(index_len).ok_or(Error::Corrupt("bad index"))?;
        let bloom_start = index_start.checked_sub(bloom_len).ok_or(Error::Corrupt("bad bloom"))?;
        if data_end as usize != bloom_start {
            return Err(Error::Corrupt("sst layout mismatch"));
        }

        let bloom = Bloom::from_bytes(&raw[bloom_start..bloom_start + bloom_len])?;
        let index_bytes = &raw[index_start..index_start + index_len];
        let mut off = 0usize;
        if index_bytes.len() < 4 {
            return Err(Error::Corrupt("bad index"));
        }
        let n = u32::from_le_bytes(index_bytes[0..4].try_into().unwrap()) as usize;
        off += 4;
        let mut index = Vec::with_capacity(n);
        for _ in 0..n {
            if off + 4 > index_bytes.len() {
                return Err(Error::Corrupt("bad index entry"));
            }
            let kl = u32::from_le_bytes(index_bytes[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            if off + kl + 8 > index_bytes.len() {
                return Err(Error::Corrupt("bad index entry key"));
            }
            let key = index_bytes[off..off + kl].to_vec();
            off += kl;
            let pos = u64::from_le_bytes(index_bytes[off..off + 8].try_into().unwrap());
            off += 8;
            index.push((key, pos));
        }

        Ok(Self {
            meta,
            data: raw,
            data_end,
            index,
            bloom,
        })
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.may_contain(key) {
            return Ok(None);
        }
        if key < self.meta.min_key.as_slice() || key > self.meta.max_key.as_slice() {
            return Ok(None);
        }

        let start = self
            .index
            .partition_point(|(k, _)| k.as_slice() <= key);
        let scan_from = if start == 0 {
            0u64
        } else {
            self.index[start - 1].1
        };

        let mut pos = scan_from as usize;
        while pos + 8 <= self.data_end as usize {
            let kl = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap()) as usize;
            let vl = u32::from_le_bytes(self.data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += 8;
            if pos + kl + vl > self.data_end as usize {
                break;
            }
            let k = &self.data[pos..pos + kl];
            pos += kl;
            let v = &self.data[pos..pos + vl];
            pos += vl;
            match k.cmp(key) {
                Ordering::Equal => return Ok(Some(v.to_vec())),
                Ordering::Greater => return Ok(None),
                Ordering::Less => {}
            }
        }
        Ok(None)
    }

    pub fn iter(&self) -> SstIter<'_> {
        SstIter {
            data: &self.data[..self.data_end as usize],
            pos: 0,
        }
    }
}

pub struct SstIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for SstIter<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 8 > self.data.len() {
            return None;
        }
        let kl = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().ok()?) as usize;
        let vl = u32::from_le_bytes(self.data[self.pos + 4..self.pos + 8].try_into().ok()?) as usize;
        self.pos += 8;
        if self.pos + kl + vl > self.data.len() {
            return None;
        }
        let k = self.data[self.pos..self.pos + kl].to_vec();
        self.pos += kl;
        let v = self.data[self.pos..self.pos + vl].to_vec();
        self.pos += vl;
        Some((k, v))
    }
}
