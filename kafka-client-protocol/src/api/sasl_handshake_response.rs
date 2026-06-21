//! Auto-generated from Kafka protocol
//! Message: SaslHandshakeResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 17,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "none"
)]
pub struct SaslHandshakeResponse {
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The mechanisms enabled in the server.
    #[kafka(versions = "0+")]
    pub mechanisms: Vec<String>,
}
