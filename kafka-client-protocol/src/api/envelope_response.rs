//! Auto-generated from Kafka protocol
//! Message: EnvelopeResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 58,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct EnvelopeResponse {
    /// The embedded response header and data.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub response_data: Option<Bytes>,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
