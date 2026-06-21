//! Auto-generated from Kafka protocol
//! Message: AddRaftVoterResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 80,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct AddRaftVoterResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}
