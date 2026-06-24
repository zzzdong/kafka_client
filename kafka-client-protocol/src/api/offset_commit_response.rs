//! Auto-generated from Kafka protocol
//! Message: OffsetCommitResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetCommitResponsePartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetCommitResponseTopic {
    /// The topic name.
    #[kafka(versions = "0-9")]
    pub name: String,
    /// The topic ID.
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    /// The responses for each partition in the topic.
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 8,
    msg_type = "response",
    valid_versions = "2-10",
    flexible_versions = "8+"
)]
pub struct OffsetCommitResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "3+")]
    pub throttle_time_ms: i32,
    /// The responses for each topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetCommitResponseTopic>,
}
