//! Auto-generated from Kafka protocol
//! Message: UnregisterBrokerResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 64, msg_type = "response", valid_versions = "0", flexible_versions = "0+")]
pub struct UnregisterBrokerResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or `null` if there was no top-level error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}

