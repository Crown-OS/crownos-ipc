use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("XDG_RUNTIME_DIR is not set or unusable")]
    RuntimeDir,
    #[error("invalid service name '{0}'")]
    InvalidServiceName(String),
    #[error("service '{service}' unavailable at {path}")]
    ServiceUnavailable { service: String, path: PathBuf },
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    #[error("payload decode failed: {0}")]
    Decode(#[from] postcard::Error),
    #[error("peer not authorized (uid {uid})")]
    Unauthorized { uid: u32 },
    #[error("peer disconnected")]
    Disconnected,
    #[error("write queue full")]
    Backpressure,
    #[error("timed out")]
    Timeout,
    #[error("remote error: {0}")]
    Remote(#[from] RemoteError),
}

/// Application-level error carried in an `Error` frame.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{message} (code {code})")]
pub struct RemoteError {
    pub code: u32,
    pub message: String,
}

impl RemoteError {
    pub const UNKNOWN_METHOD: u32 = 1;
    pub const HANDLER: u32 = 2;
    pub const UNAUTHORIZED: u32 = 3;
    pub const LIMIT: u32 = 4;

    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn unknown_method(selector: u64) -> Self {
        Self::new(
            Self::UNKNOWN_METHOD,
            format!("unknown method selector {selector:#018x}"),
        )
    }

    pub fn handler(message: impl Into<String>) -> Self {
        Self::new(Self::HANDLER, message)
    }
}
