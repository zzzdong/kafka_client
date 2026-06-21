//! Auto-generated from Kafka protocol
//! Message: AlterConfigsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterConfigsResourceResponse {
    /// The resource error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The resource error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The resource type.
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    /// The resource name.
    #[kafka(versions = "0+")]
    pub resource_name: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 33,
    msg_type = "response",
    valid_versions = "0-2",
    flexible_versions = "2+"
)]
pub struct AlterConfigsResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The responses for each resource.
    #[kafka(versions = "0+")]
    pub responses: Vec<AlterConfigsResourceResponse>,
}
