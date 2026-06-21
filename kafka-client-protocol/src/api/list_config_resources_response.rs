//! Auto-generated from Kafka protocol
//! Message: ListConfigResourcesResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ConfigResource {
    /// The resource name.
    #[kafka(versions = "0+")]
    pub resource_name: String,
    /// The resource type.
    #[kafka(versions = "1+", nullable_versions = "1+", default = 16)]
    pub resource_type: i8,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 74,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct ListConfigResourcesResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// Each config resource in the response.
    #[kafka(versions = "0+")]
    pub config_resources: Vec<ConfigResource>,
}
