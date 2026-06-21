//! Auto-generated from Kafka protocol
//! Message: ControllerRegistrationResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 70,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct ControllerRegistrationResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The response error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The response error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}
