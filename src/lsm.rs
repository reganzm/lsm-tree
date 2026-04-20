//! Minimal leveled LSM: memtable + WAL + per-level SSTs + background-style compaction.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::compact::{merge_sorted_runs, write_level};
use crate::error::Result;
use crate::manifest::Manifest;
use crate::sst::{SstMeta, SstReader};
use crate::wal::Wal;

const WAL_NAME: &str = "wal.log";

/// Level `L` is compacted into `L+1` when its total byte size exceeds `level_base_bytes * (multiplier^L)`.
#[derive(Clone, Debug)]
pub struct Options {
    pub memtable_max_bytes: usize,
    pub l0_max_files: usize,
    pub target_sst_bytes: usize,
    pub max_levels: usize,
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_max_bytes: 256 * 1024,
            l0_max_files: 4,
            target_sst_bytes: 64 * 1024,
            max_levels: 8,
            level_base_bytes: 256 * 1024,
            level_multiplier: 10,
        }
    }
}

pub struct Lsm {
    dir: PathBuf,
    opts: Options,
    mem: BTreeMap<Vec<u8>, Vec<u8>>,
    mem_bytes: usize,
    wal: Wal,
    manifest: Manifest,
    /// Open readers per level (lazy reload after compaction).
    levels: Vec<Vec<SstReader>>,
}

impl Lsm {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut wal = Wal::open(dir.join(WAL_NAME))?;
        let mut mem: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut mem_bytes = 0usize;
        wal.replay(|k, v| {
            let klen = k.len();
            let vlen = v.len();
            match mem.insert(k, v) {
                None => mem_bytes += klen + vlen,
                Some(old_v) => mem_bytes += vlen.saturating_sub(old_v.len()),
            }
        })?;

        let manifest = Manifest::load(&dir)?;
        let levels = load_all_readers(&manifest)?;

        let mut s = Self {
            dir,
            opts,
            mem,
            mem_bytes,
            wal,
            manifest,
            levels,
        };
        s.maybe_compact()?;
        Ok(s)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.get(key) {
            return Ok(Some(v.clone()));
        }
        for (li, level) in self.levels.iter().enumerate() {
            let mut order: Vec<usize> = (0..level.len()).collect();
            if li == 0 {
                order.sort_by(|&a, &b| {
                    level[b]
                        .meta
                        .seq
                        .cmp(&level[a].meta.seq)
                });
            }
            for idx in order {
                let r = &level[idx];
                if key < r.meta.min_key.as_slice() || key > r.meta.max_key.as_slice() {
                    continue;
                }
                if let Some(v) = r.get(key)? {
                    return Ok(Some(v));
                }
            }
        }
        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let k = key.to_vec();
        let v = value.to_vec();
        self.wal.append_put(&k, &v)?;

        let add = k.len() + v.len();
        if let Some(old) = self.mem.insert(k, v) {
            self.mem_bytes -= old.len();
        }
        self.mem_bytes += add;

        if self.mem_bytes >= self.opts.memtable_max_bytes {
            self.flush_memtable()?;
        }
        self.maybe_compact()?;
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }

        let rows: Vec<(Vec<u8>, Vec<u8>)> = self.mem.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        self.mem.clear();
        self.mem_bytes = 0;

        let id = self.manifest.next_id;
        let seq = self.manifest.next_seq;
        self.manifest.next_id += 1;
        self.manifest.next_seq += 1;

        while self.manifest.levels.len() < 1 {
            self.manifest.levels.push(Vec::new());
        }

        let path = self.dir.join(format!("sst_{}_{}.sst", seq, 0));
        let mut w = crate::sst::SstWriter::create(&path)?;
        for (k, v) in &rows {
            w.write_kv(k, v)?;
        }
        let meta = w.finish(id, seq)?;
        self.manifest.levels[0].push(meta.clone().into());
        self.manifest.save(&self.dir)?;

        let reader = SstReader::open(meta)?;
        self.levels[0].push(reader);

        self.wal.reset()?;
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<()> {
        loop {
            if self.manifest.levels.is_empty() {
                break;
            }
            let l0 = self.manifest.levels.get(0).map(|x| x.len()).unwrap_or(0);
            if l0 > self.opts.l0_max_files {
                self.compact_level(0)?;
                continue;
            }

            let mut did = false;
            for level in 0..self.manifest.levels.len() {
                if level + 1 >= self.opts.max_levels {
                    break;
                }
                let limit = self
                    .opts
                    .level_base_bytes
                    .saturating_mul(self.opts.level_multiplier.saturating_pow(level as u32));
                let metas = &self.manifest.levels[level];
                let bytes: u64 = metas.iter().map(|r| r.size_bytes).sum();
                if !metas.is_empty() && bytes > limit {
                    self.compact_level(level)?;
                    did = true;
                    break;
                }
            }
            if !did {
                break;
            }
        }
        Ok(())
    }

    /// Merge all SSTs at `level` into the next level as non-overlapping sized runs.
    fn compact_level(&mut self, level: usize) -> Result<()> {
        let next = level + 1;
        if next >= self.opts.max_levels {
            return Ok(());
        }

        while self.manifest.levels.len() <= next {
            self.manifest.levels.push(Vec::new());
        }

        let take = self.manifest.levels[level].clone();
        if take.is_empty() {
            return Ok(());
        }

        let mut runs: Vec<(u64, Vec<(Vec<u8>, Vec<u8>)>)> = Vec::new();
        for rec in take {
            let meta: SstMeta = rec.try_into()?;
            let r = SstReader::open(meta.clone())?;
            let rows: Vec<_> = r.iter().collect();
            runs.push((meta.seq, rows));
        }

        let merged = merge_sorted_runs(runs);

        for rec in &self.manifest.levels[level] {
            let _ = fs::remove_file(self.dir.join(&rec.path));
        }
        self.manifest.levels[level].clear();

        let id_base = self.manifest.next_id;
        let seq_base = self.manifest.next_seq;
        let new_metas = write_level(
            &self.dir,
            &merged,
            self.opts.target_sst_bytes,
            id_base,
            seq_base,
        )?;

        self.manifest.next_id += new_metas.len() as u64;
        self.manifest.next_seq += new_metas.len() as u64;

        for m in new_metas {
            self.manifest.levels[next].push(m.into());
        }

        self.manifest.save(&self.dir)?;
        self.levels = load_all_readers(&self.manifest)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }
}

fn load_all_readers(m: &Manifest) -> Result<Vec<Vec<SstReader>>> {
    let mut out = Vec::new();
    for level in &m.levels {
        let mut lv = Vec::new();
        for rec in level {
            let meta: SstMeta = rec.clone().try_into()?;
            lv.push(SstReader::open(meta)?);
        }
        out.push(lv);
    }
    Ok(out)
}
