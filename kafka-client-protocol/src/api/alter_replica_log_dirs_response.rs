//! Auto-generated from Kafka protocol
//! Message: AlterReplicaLogDirsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterReplicaLogDirPartitionResult {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterReplicaLogDirTopicResult {
    /// The name of the topic.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The results for each partition.
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterReplicaLogDirPartitionResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 34,
    msg_type = "response",
    valid_versions = "1-2",
    flexible_versions = "2+"
)]
pub struct AlterReplicaLogDirsResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The results for each topic.
    #[kafka(versions = "0+")]
    pub results: Vec<AlterReplicaLogDirTopicResult>,
}
