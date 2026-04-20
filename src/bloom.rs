//! Tiny Bloom filter (FNV-1a style hashes) for negative lookups.

use crate::error::{Error, Result};

const DEFAULT_BITS: usize = 512;
const NUM_HASHES: usize = 4;

fn hash(seed: u64, data: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64 ^ seed;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub struct Bloom {
    bits: Vec<u64>,
}

impl Bloom {
    pub fn new() -> Self {
        Self {
            bits: vec![0u64; DEFAULT_BITS / 64],
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        let n = self.bits.len() * 64;
        for i in 0..NUM_HASHES {
            let h = hash(i as u64, key) as usize % n;
            self.bits[h / 64] |= 1 << (h % 64);
        }
    }

    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        if raw.len() % 8 != 0 {
            return Err(Error::Corrupt("bad bloom length"));
        }
        let mut bits = Vec::with_capacity(raw.len() / 8);
        for chunk in raw.chunks_exact(8) {
            bits.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Ok(Self { bits })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.bits.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let n = self.bits.len() * 64;
        for i in 0..NUM_HASHES {
            let h = hash(i as u64, key) as usize % n;
            if self.bits[h / 64] & (1 << (h % 64)) == 0 {
                return false;
            }
        }
        true
    }
}
