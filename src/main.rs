//! Demo: set/get + optional range scan via memtable inspection.

use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use lsm_tree::{Lsm, Options};

fn main() -> Result<()> {
    let dir = PathBuf::from("data_demo");
    let _ = fs::remove_dir_all(&dir);

    let opts = Options {
        memtable_max_bytes: 512,
        l0_max_files: 2,
        target_sst_bytes: 400,
        max_levels: 6,
        level_base_bytes: 300,
        level_multiplier: 4,
    };

    let mut db = Lsm::open(&dir, opts.clone())?;

    for i in 0..30 {
        let k = format!("k{i:02}");
        let v = format!("v{i}");
        db.set(k.as_bytes(), v.as_bytes())?;
    }

    assert_eq!(db.get(b"k05")?.as_deref(), Some(b"v5".as_slice()));
    assert_eq!(db.get(b"missing")?, None);

    let dir = db.path().to_path_buf();
    drop(db);

    let db2 = Lsm::open(&dir, opts.clone())?;
    assert_eq!(db2.get(b"k20")?.as_deref(), Some(b"v20".as_slice()));

    println!("LSM demo OK. Data directory: {}", dir.display());
    Ok(())
}
