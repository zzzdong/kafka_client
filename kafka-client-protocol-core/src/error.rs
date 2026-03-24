// src/error.rs
use std::io;
use thiserror::Error;
use bytes::Buf;

/// 协议错误类型
/// 
/// 涵盖 Kafka 协议解析过程中可能出现的所有错误
#[derive(Debug, Error)]
pub enum ProtocolError {
    // ============ IO 相关错误 ============
    
    /// IO 错误（网络、文件等）
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    // ============ 数据完整性错误 ============
    
    /// 数据不足，无法完成解码
    #[error("Insufficient data: expected {expected} bytes, got {actual}")]
    InsufficientData {
        expected: usize,
        actual: usize,
    },
    
    /// 数据超出预期大小
    #[error("Data too large: expected max {max}, got {actual}")]
    DataTooLarge {
        max: usize,
        actual: usize,
    },
    
    /// CRC 校验失败
    #[error("CRC checksum failed: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch {
        expected: u32,
        actual: u32,
    },
    
    /// 无效的数据格式
    #[error("Invalid data format: {0}")]
    InvalidData(String),
    
    // ============ 版本相关错误 ============
    
    /// 不支持的 API 版本
    #[error("Unsupported API version: {version} for API key {api_key}")]
    UnsupportedVersion {
        api_key: i16,
        version: i16,
    },
    
    /// 不支持的 API Key
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),
    
    /// 版本协商失败
    #[error("Version negotiation failed: {0}")]
    VersionNegotiationFailed(String),
    
    // ============ 编解码错误 ============
    
    /// 编码错误
    #[error("Encode error: {0}")]
    Encode(String),
    
    /// 解码错误
    #[error("Decode error: {0}")]
    Decode(String),
    
    /// 未知的字段标签
    #[error("Unknown tagged field tag: {0}")]
    UnknownTag(u32),
    
    // ============ UTF-8 相关错误 ============
    
    /// UTF-8 解码错误
    #[error("UTF-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    
    /// 字符串包含 NUL 字符
    #[error("String contains NUL character: {0}")]
    StringContainsNul(String),
    
    // ============ 其他错误 ============
    
    /// 其他协议错误
    #[error("Protocol error: {0}")]
    Other(String),
}

/// 协议操作结果类型
pub type ProtocolResult<T> = Result<T, ProtocolError>;

impl ProtocolError {
    /// 创建无效数据错误
    pub fn invalid_data(msg: impl Into<String>) -> Self {
        ProtocolError::InvalidData(msg.into())
    }
    
    /// 创建数据不足错误
    pub fn insufficient_data(expected: usize, actual: usize) -> Self {
        ProtocolError::InsufficientData { expected, actual }
    }
    
    /// 创建不支持的版本错误
    pub fn unsupported_version(api_key: i16, version: i16) -> Self {
        ProtocolError::UnsupportedVersion { api_key, version }
    }
    
    /// 检查错误是否可重试
    pub fn is_retryable(&self) -> bool {
        match self {
            // IO 错误可能可重试
            ProtocolError::Io(e) => {
                // 连接重置、超时等可重试
                e.kind() == io::ErrorKind::ConnectionReset
                    || e.kind() == io::ErrorKind::TimedOut
                    || e.kind() == io::ErrorKind::BrokenPipe
            }
            // 数据不足可能是部分读取，可重试
            ProtocolError::InsufficientData { .. } => true,
            // 其他错误通常不可重试
            _ => false,
        }
    }
    
    /// 获取错误码（用于 Kafka 协议）
    pub fn error_code(&self) -> Option<i16> {
        match self {
            ProtocolError::UnsupportedVersion { .. } => Some(35), // UNSUPPORTED_VERSION
            ProtocolError::UnsupportedApiKey(_) => Some(35),     // UNSUPPORTED_VERSION
            ProtocolError::InvalidData(_) => Some(42),           // INVALID_REQUEST
            ProtocolError::CrcMismatch { .. } => Some(2),        // CORRUPT_MESSAGE
            _ => None,
        }
    }
}

// ============ 便捷宏 ============

/// 确保缓冲区有足够的数据
#[macro_export]
macro_rules! ensure_remaining {
    ($buf:expr, $len:expr) => {
        if $buf.remaining() < $len {
            return Err($crate::ProtocolError::insufficient_data(
                $len,
                $buf.remaining(),
            ));
        }
    };
}

/// 确保条件成立，否则返回错误
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
}

// ============ 错误转换 ============

impl From<std::str::Utf8Error> for ProtocolError {
    fn from(err: std::str::Utf8Error) -> Self {
        ProtocolError::InvalidData(format!("Invalid UTF-8: {}", err))
    }
}

// impl From<serde_json::Error> for ProtocolError {
//     fn from(err: serde_json::Error) -> Self {
//         ProtocolError::InvalidData(format!("JSON error: {}", err))
//     }
// }

// impl From<base64::DecodeError> for ProtocolError {
//     fn from(err: base64::DecodeError) -> Self {
//         ProtocolError::InvalidData(format!("Base64 decode error: {}", err))
//     }
// }

impl From<&str> for ProtocolError {
    fn from(s: &str) -> Self {
        ProtocolError::Other(s.to_string())
    }
}

impl From<String> for ProtocolError {
    fn from(s: String) -> Self {
        ProtocolError::Other(s)
    }
}

// ============ 测试 ============

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_creation() {
        let err = ProtocolError::invalid_data("test error");
        assert!(matches!(err, ProtocolError::InvalidData(_)));
        assert_eq!(err.error_code(), Some(42));
        assert!(!err.is_retryable());
    }
    
    #[test]
    fn test_insufficient_data() {
        let err = ProtocolError::insufficient_data(100, 50);
        assert!(matches!(err, ProtocolError::InsufficientData { expected: 100, actual: 50 }));
        assert!(err.is_retryable());
    }
    
    #[test]
    fn test_unsupported_version() {
        let err = ProtocolError::unsupported_version(3, 99);
        assert!(matches!(err, ProtocolError::UnsupportedVersion { api_key: 3, version: 99 }));
        assert_eq!(err.error_code(), Some(35));
    }
}