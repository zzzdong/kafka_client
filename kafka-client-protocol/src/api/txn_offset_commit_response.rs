//! Auto-generated from Kafka protocol
//! Message: TxnOffsetCommitResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TxnOffsetCommitResponsePartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TxnOffsetCommitResponseTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The responses for each partition in the topic.
    #[kafka(versions = "0+")]
    pub partitions: Vec<TxnOffsetCommitResponsePartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 28,
    msg_type = "response",
    valid_versions = "0-5",
    flexible_versions = "3+"
)]
pub struct TxnOffsetCommitResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The responses for each topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<TxnOffsetCommitResponseTopic>,
}
