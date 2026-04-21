//! 大规模数据测试：默认忽略，请在 release 下运行（`--release` 必须写在 `cargo test` 后、`--` 前）。
//!
//! `cargo test` 会**捕获** stdout/stderr，成功时默认看不到 `eprintln!`；要看进度请加 **`--nocapture`**（写在 `--` 之后）：
//!
//! ```text
//! cargo test --release --test million_records million_roundtrip -- --ignored --nocapture
//! ```
//!
//! 如需在 debug 下跑（较慢），去掉 `--release`。
//!
//! 合并路径验证（小阈值、触发 `L0→L1`）：
//! `cargo test --release --test million_records leveled_compaction_smoke -- --ignored`

use std::fs;
use std::path::PathBuf;

use lsm_tree::{Lsm, Options};

const TOTAL: u64 = 1_000_000;

fn bulk_options() -> Options {
    Options {
        memtable_max_bytes: 4 * 1024 * 1024,
        l0_max_files: 8,
        target_sst_bytes: 2 * 1024 * 1024,
        max_levels: 10,
        level_base_bytes: 32 * 1024 * 1024,
        level_multiplier: 8,
    }
}

/// 小 memtable + 低 L0 上限，少量写入即可触发 `L0 -> L1` 合并。
fn compact_smoke_options() -> Options {
    Options {
        memtable_max_bytes: 512,
        l0_max_files: 2,
        target_sst_bytes: 2048,
        max_levels: 8,
        level_base_bytes: 64 * 1024 * 1024,
        level_multiplier: 8,
    }
}

fn key(i: u64) -> [u8; 8] {
    i.to_le_bytes()
}

fn val(i: u64) -> [u8; 8] {
    i.to_le_bytes()
}

#[test]
fn helpers_and_options_smoke() {
    let _ = bulk_options();
    assert_eq!(
        u64::from_le_bytes(key(0x1122_3344_5566_7788)),
        0x1122_3344_5566_7788
    );
}

/// 使用 `target/lsm_test_runs/`（随 `target/` 已被 gitignore），避免 Windows 下
/// `%TEMP%` 被 OneDrive/杀毒频繁加锁导致 `PermissionDenied`。
fn temp_db_dir(label: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("target");
    p.push("lsm_test_runs");
    p.push(format!(
        "{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = fs::remove_dir_all(&p);
    p
}

fn assert_get(db: &Lsm, i: u64) {
    let got = db.get(&key(i)).expect("get");
    assert_eq!(got.as_deref(), Some(val(i).as_slice()), "key {i}");
}

/// 每条 mem 约 16 字节 → 512 字节 memtable ≈ 32 条/flush；`l0_max_files == 2` 时
/// 第 3 个 L0 SST 会触发 `compact_level(0)`。写入略多于 `3×32` 条即可。
///
/// Debug + Windows 上可能较慢；默认忽略，请用：
/// `cargo test --release --test million_records leveled_compaction_smoke -- --ignored`
#[test]
#[ignore = "IO: cargo test --release --test million_records leveled_compaction_smoke -- --ignored"]
fn leveled_compaction_smoke() {
    const N: u64 = 10000;
    let dir = temp_db_dir("compact");
    fs::create_dir_all(&dir).expect("mkdir");

    {
        let mut db = Lsm::open(&dir, compact_smoke_options()).expect("open");
        for i in 0u64..N {
            db.set(&key(i), &val(i)).expect("set");
            println!("inserted {i} / {N}");
        }
        assert_get(&db, 0);
        assert_get(&db, N - 1);
        assert_get(&db, 42);
    }

    let raw = fs::read_to_string(dir.join("MANIFEST.json")).expect("read manifest");
    let m: serde_json::Value = serde_json::from_str(&raw).expect("manifest json");
    let levels = m["levels"].as_array().expect("levels");
    assert!(
        levels.len() >= 2,
        "合并后应至少存在 L0、L1 两层: {levels:?}"
    );
    let l1 = levels[1].as_array().expect("L1");
    assert!(
        !l1.is_empty(),
        "L1 上应有合并后的 SST: {levels:?}"
    );

    {
        let db = Lsm::open(&dir, compact_smoke_options()).expect("reopen");
        assert_get(&db, 0);
        assert_get(&db, N - 1);
    }

   //let _ = fs::remove_dir_all(&dir);
}

/// 写入并抽查；关闭重开后再次抽查。
#[test]
#[ignore = "slow: cargo test --release --test million_records million_roundtrip -- --ignored --nocapture"]
fn million_roundtrip() {
    let dir = temp_db_dir("million");
    fs::create_dir_all(&dir).expect("mkdir");

    {
        let mut db = Lsm::open(&dir, bulk_options()).expect("open");

        for i in 0u64..TOTAL {
            db.set(&key(i), &val(i)).expect("set");
            if i > 0 && i % 100 == 0 {
                eprintln!("  inserted {i} / {TOTAL}");
            }
        }

        assert_get(&db, 0);
        assert_get(&db, TOTAL - 1);
        assert_get(&db, 123_456);
        assert_get(&db, 789_012);
        assert_get(&db, 500_000);

        for step in [1u64, 97, 999, 10_007, 100_003, 333_333, 777_777] {
            assert_get(&db, step);
        }

        assert_eq!(
            db.get(&0xFFFF_FFFF_FFFF_FFFFu64.to_le_bytes()).expect("get"),
            None
        );
    }

    eprintln!("  reopening...");
    {
        let db = Lsm::open(&dir, bulk_options()).expect("reopen");
        assert_get(&db, 0);
        assert_get(&db, TOTAL - 1);
        assert_get(&db, 42);
        assert_get(&db, 999_999);
    }

    //let _ = fs::remove_dir_all(&dir);
}
