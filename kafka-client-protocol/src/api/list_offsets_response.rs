//! Auto-generated from Kafka protocol
//! Message: ListOffsetsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ListOffsetsPartitionResponse {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The timestamp associated with the returned offset.
    #[kafka(versions = "1+", default = -1)]
    pub timestamp: i64,
    /// The returned offset.
    #[kafka(versions = "1+", default = -1)]
    pub offset: i64,
    /// The leader epoch associated with the returned offset.
    #[kafka(versions = "4+", default = -1)]
    pub leader_epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ListOffsetsTopicResponse {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// Each partition in the response.
    #[kafka(versions = "0+")]
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 2,
    msg_type = "response",
    valid_versions = "1-11",
    flexible_versions = "6+"
)]
pub struct ListOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "2+", nullable_versions = "2+")]
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    #[kafka(versions = "0+")]
    pub topics: Vec<ListOffsetsTopicResponse>,
}
