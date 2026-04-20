use thiserror::Error;

/// 库内部与各层 API 使用的具体错误类型（`thiserror`）。
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("corrupt: {0}")]
    Corrupt(&'static str),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
