use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::sst::SstMeta;

const MANIFEST_NAME: &str = "MANIFEST.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub next_id: u64,
    pub next_seq: u64,
    pub levels: Vec<Vec<SstRecord>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SstRecord {
    pub path: String,
    pub id: u64,
    pub seq: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub size_bytes: u64,
}

impl From<SstMeta> for SstRecord {
    fn from(m: SstMeta) -> Self {
        SstRecord {
            path: m.path.to_string_lossy().into_owned(),
            id: m.id,
            seq: m.seq,
            min_key: m.min_key,
            max_key: m.max_key,
            size_bytes: m.size_bytes,
        }
    }
}

impl TryFrom<SstRecord> for SstMeta {
    type Error = crate::error::Error;

    fn try_from(r: SstRecord) -> Result<Self> {
        Ok(SstMeta {
            path: PathBuf::from(r.path),
            id: r.id,
            seq: r.seq,
            min_key: r.min_key,
            max_key: r.max_key,
            size_bytes: r.size_bytes,
        })
    }
}

impl Manifest {
    pub fn empty() -> Self {
        Self {
            next_id: 1,
            next_seq: 1,
            levels: Vec::new(),
        }
    }

    pub fn load(dir: &Path) -> Result<Self> {
        let p = dir.join(MANIFEST_NAME);
        if !p.exists() {
            return Ok(Self::empty());
        }
        let bytes = fs::read(&p)?;
        let m: Manifest = serde_json::from_slice(&bytes)?;
        Ok(m)
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        let p = dir.join(MANIFEST_NAME);
        let tmp = dir.join("MANIFEST.json.tmp");
        let data = serde_json::to_vec_pretty(self)?;
        fs::write(&tmp, &data)?;
        fs::rename(&tmp, &p)?;
        Ok(())
    }
}
