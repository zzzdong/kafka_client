use std::fmt;
use thiserror::Error;

// 重新导出 protocol 模块的错误类型
pub use kafka_client_protocol::ProtocolError;

/// Error connecting to a specific broker.
#[derive(Debug, Clone)]
pub struct BrokerConnError {
    /// Broker address (host:port).
    pub addr: String,
    /// The underlying error.
    pub error: KafkaError,
}

impl fmt::Display for BrokerConnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.addr, self.error)
    }
}

/// A list of broker connection errors, displayed as a semicolon-separated list.
#[derive(Debug, Clone)]
pub struct BrokerErrors(pub Vec<BrokerConnError>);

impl fmt::Display for BrokerErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{}", e)?;
        }
        Ok(())
    }
}

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

    #[error("Mechanism not supported: {0}")]
    MechanismNotSupported(String),

    #[error("No bootstrap broker available: {0}")]
    NoBootstrapBrokerAvailable(BrokerErrors),

    #[error("No broker available: {0}")]
    NoBrokerAvailable(BrokerErrors),

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

    #[error("TLS handshake failed for {addr}: {details}")]
    TlsError { addr: String, details: String },

    #[error("SASL error: {0}")]
    SaslError(#[from] SaslError),

    #[error("Protocol decode error: {0}")]
    ProtocolError(String),

    /// Insufficient data for protocol decode.
    #[error("Insufficient protocol data: expected {expected} bytes, got {actual}")]
    InsufficientData { expected: usize, actual: usize },

    /// Protocol data exceeds allowed maximum.
    #[error("Protocol data too large: max {max}, actual {actual}")]
    DataTooLarge { max: usize, actual: usize },

    /// CRC checksum mismatch.
    #[error("CRC checksum mismatch: expected {expected:#x}, actual {actual:#x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Correlation ID mismatch: expected {expected}, actual {actual}")]
    CorrelationIdMismatch { expected: i32, actual: i32 },

    #[error("Group rebalance required")]
    RebalanceRequired,

    #[error("Illegal group generation: {0}")]
    IllegalGeneration(i32),

    #[error("Unknown member id: {0}")]
    UnknownMemberId(String),
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
        match e {
            ProtocolError::InsufficientData { expected, actual } => {
                KafkaError::InsufficientData { expected, actual }
            }
            ProtocolError::DataTooLarge { max, actual } => KafkaError::DataTooLarge { max, actual },
            ProtocolError::CrcMismatch { expected, actual } => {
                KafkaError::CrcMismatch { expected, actual }
            }
            e => KafkaError::ProtocolError(e.to_string()),
        }
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
