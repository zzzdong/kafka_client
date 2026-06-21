//! Auto-generated from Kafka protocol
//! Message: AlterPartitionReassignmentsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReassignablePartitionResponse {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code for this partition, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message for this partition, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReassignableTopicResponse {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The responses to partitions to reassign.
    #[kafka(versions = "0+")]
    pub partitions: Vec<ReassignablePartitionResponse>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 45,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct AlterPartitionReassignmentsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The option indicating whether changing the replication factor of any given partition as part of the request was allowed.
    #[kafka(versions = "1+", nullable_versions = "1+", default = true)]
    pub allow_replication_factor_change: bool,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The responses to topics to reassign.
    #[kafka(versions = "0+")]
    pub responses: Vec<ReassignableTopicResponse>,
}
