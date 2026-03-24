//! Auto-generated from Kafka protocol
//! Message: SaslAuthenticateResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 36, msg_type = "response", valid_versions = "0-2", flexible_versions = "2+")]
pub struct SaslAuthenticateResponse {
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    #[kafka(versions = "0+")]
    pub auth_bytes: Bytes,
    /// Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
    #[kafka(versions = "1+", nullable_versions = "1+", default = 0)]
    pub session_lifetime_ms: i64,
}

