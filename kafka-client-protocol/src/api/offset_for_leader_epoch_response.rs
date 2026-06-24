//! Auto-generated from Kafka protocol
//! Message: OffsetForLeaderEpochResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EpochEndOffset {
    /// The error code 0, or if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The leader epoch of the partition.
    #[kafka(versions = "1+", default = -1)]
    pub leader_epoch: i32,
    /// The end offset of the epoch.
    #[kafka(versions = "0+", default = -1)]
    pub end_offset: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetForLeaderTopicResult {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic: String,
    /// Each partition in the topic we fetched offsets for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<EpochEndOffset>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 23,
    msg_type = "response",
    valid_versions = "2-4",
    flexible_versions = "4+"
)]
pub struct OffsetForLeaderEpochResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "2+")]
    pub throttle_time_ms: i32,
    /// Each topic we fetched offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetForLeaderTopicResult>,
}
