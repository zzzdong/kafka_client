use thiserror::Error;
use std::io;

/// 协议错误类型
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// 数据不足，无法完整解码
    #[error("Insufficient data: expected {expected}, got {actual}")]
    InsufficientData { expected: usize, actual: usize },

    /// 无效的数据格式
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// 不支持的版本
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(i16),

    /// 不支持的 API Key
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),

    /// 编码/解码错误
    #[error("Codec error: {0}")]
    Codec(String),

    /// IO 错误
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// UTF-8 解码错误
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    /// Base64 解码错误
    #[error("Base64 error: {0}")]
    Base64(#[from] base64::DecodeError),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;
