//! Level merge: merge sorted runs and deduplicate by newest `seq`.

use crate::error::Result;
use crate::sst::{SstMeta, SstWriter};

/// Merge tagged rows from several SSTs; same key keeps the row from the highest `seq`.
pub fn merge_sorted_runs(
    runs: Vec<(u64, Vec<(Vec<u8>, Vec<u8>)>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut tagged: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    for (seq, rows) in runs {
        for (k, v) in rows {
            tagged.push((k, v, seq));
        }
    }
    tagged.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| b.2.cmp(&a.2)) // newer seq first
    });
    let mut out = Vec::with_capacity(tagged.len());
    for (k, v, _) in tagged {
        if out.last().map(|(pk, _)| pk == &k).unwrap_or(false) {
            continue;
        }
        out.push((k, v));
    }
    out
}

/// Split sorted rows into SSTs of roughly `target_bytes` each.
pub fn write_level(
    dir: &std::path::Path,
    rows: &[(Vec<u8>, Vec<u8>)],
    target_bytes: usize,
    id_start: u64,
    seq_start: u64,
) -> Result<Vec<SstMeta>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut metas = Vec::new();
    let mut i = 0usize;
    let mut file_idx = 0u64;
    while i < rows.len() {
        let path = dir.join(format!("sst_{}_{}.sst", seq_start, file_idx));
        let mut w = SstWriter::create(&path)?;
        let mut size = 0usize;
        let id = id_start + file_idx;
        let seq = seq_start + file_idx;

        while i < rows.len() {
            let (k, v) = &rows[i];
            let row = 8 + k.len() + v.len();
            if size > 0 && size + row > target_bytes {
                break;
            }
            w.write_kv(k, v)?;
            size += row;
            i += 1;
        }
        metas.push(w.finish(id, seq)?);
        file_idx += 1;
    }
    Ok(metas)
}

