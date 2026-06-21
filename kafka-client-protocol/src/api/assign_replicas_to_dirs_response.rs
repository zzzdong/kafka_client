//! Auto-generated from Kafka protocol
//! Message: AssignReplicasToDirsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The partition level error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The ID of the assigned topic.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The list of assigned partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DirectoryData {
    /// The ID of the directory.
    #[kafka(versions = "0+")]
    pub id: Uuid,
    /// The list of topics and their assigned partitions.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 73,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct AssignReplicasToDirsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The list of directories and their assigned partitions.
    #[kafka(versions = "0+")]
    pub directories: Vec<DirectoryData>,
}
