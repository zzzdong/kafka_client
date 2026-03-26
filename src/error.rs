use thiserror::Error;

// 重新导出 protocol 模块的错误类型
pub use kafka_client_protocol::ProtocolError;

#[derive(Debug, Error, Clone)]
pub enum KafkaError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("UTF-8 error: {0}")]
    Utf8(String),

    #[error("Base64 decode error: {0}")]
    Base64(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("SASL handshake failed: {0}")]
    SaslHandshakeFailed(String),

    #[error("Mechanism not supported: {0}")]
    MechanismNotSupported(String),

    #[error("No bootstrap broker available")]
    NoBootstrapBrokerAvailable,

    #[error("No broker available")]
    NoBrokerAvailable,

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: {0}, partition {1}")]
    PartitionNotFound(String, i32),

    #[error("Unsupported API: {0}")]
    UnsupportedApi(i16),

    #[error("Produce error: {0}")]
    ProduceError(i16),

    #[error("Offset commit error: {0}")]
    OffsetCommitError(i16),

    #[error("No offset stored")]
    NoOffsetStored,

    #[error("No coordinator available")]
    NoCoordinator,

    #[error("Offset not found: {0}, partition {1}")]
    OffsetNotFound(String, i32),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("SASL error: {0}")]
    SaslError(#[from] SaslError),

    #[error("Protocol decode error: {0}")]
    ProtocolError(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Correlation ID mismatch: expected {expected}, actual {actual}")]
    CorrelationIdMismatch { expected: i32, actual: i32 },
}

impl From<std::io::Error> for KafkaError {
    fn from(e: std::io::Error) -> Self {
        KafkaError::Io(e.to_string())
    }
}

impl From<std::string::FromUtf8Error> for KafkaError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        KafkaError::Utf8(e.to_string())
    }
}

impl From<base64::DecodeError> for KafkaError {
    fn from(e: base64::DecodeError) -> Self {
        KafkaError::Base64(e.to_string())
    }
}

impl From<ProtocolError> for KafkaError {
    fn from(e: ProtocolError) -> Self {
        KafkaError::ProtocolError(e.to_string())
    }
}

#[derive(Debug, Error, Clone)]
pub enum SaslError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Invalid challenge: {0}")]
    InvalidChallenge(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Mechanism not supported: {0}")]
    MechanismNotSupported(String),

    #[error("Invalid state")]
    InvalidState,

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

pub type Result<T> = std::result::Result<T, KafkaError>;
