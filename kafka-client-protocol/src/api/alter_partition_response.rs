//! Auto-generated from Kafka protocol
//! Message: AlterPartitionResponse
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
    /// The broker ID of the leader.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The leader epoch.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// The in-sync replica IDs.
    #[kafka(versions = "0+")]
    pub isr: Vec<i32>,
    /// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
    #[kafka(versions = "1+", default = 0)]
    pub leader_recovery_state: i8,
    /// The current epoch for the partition for KRaft controllers.
    #[kafka(versions = "0+")]
    pub partition_epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The ID of the topic.
    #[kafka(versions = "2+")]
    pub topic_id: Uuid,
    /// The responses for each partition.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 56,
    msg_type = "response",
    valid_versions = "2-3",
    flexible_versions = "0+"
)]
pub struct AlterPartitionResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The responses for each topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}
