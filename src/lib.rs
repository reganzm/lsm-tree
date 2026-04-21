//! Minimal leveled LSM-tree (educational).

pub mod bloom;
mod compact;
pub mod error;
mod io_retry;
mod lsm;
pub mod manifest;
pub mod sst;
mod wal;

/// 应用侧可把 `anyhow::Result` 与 `?` 配合使用；库内仍返回 [`Error`]。
pub use anyhow;
pub use error::Error;
pub use lsm::{Lsm, Options};
