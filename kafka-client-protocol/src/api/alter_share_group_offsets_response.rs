//! Auto-generated from Kafka protocol
//! Message: AlterShareGroupOffsetsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterShareGroupOffsetsResponsePartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterShareGroupOffsetsResponseTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic_name: String,
    /// The unique topic ID.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterShareGroupOffsetsResponsePartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 91,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct AlterShareGroupOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The results for each topic.
    #[kafka(versions = "0+")]
    pub responses: Vec<AlterShareGroupOffsetsResponseTopic>,
}
