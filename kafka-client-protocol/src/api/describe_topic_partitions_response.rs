//! Auto-generated from Kafka protocol
//! Message: DescribeTopicPartitionsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponsePartition {
    /// The partition error, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The ID of the leader broker.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The leader epoch of this partition.
    #[kafka(versions = "0+", nullable_versions = "0+", default = -1)]
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    #[kafka(versions = "0+")]
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    #[kafka(versions = "0+")]
    pub isr_nodes: Vec<i32>,
    /// The new eligible leader replicas otherwise.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// The last known ELR.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub last_known_elr: Option<Vec<i32>>,
    /// The set of offline replicas of this partition.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub offline_replicas: Option<Vec<i32>>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponseTopic {
    /// The topic error, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The topic name.
    #[kafka(versions = "0+", nullable_versions = "0+", map_key)]
    pub name: Option<String>,
    /// The topic id.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub topic_id: Option<Uuid>,
    /// True if the topic is internal.
    #[kafka(versions = "0+", nullable_versions = "0+", default = false)]
    pub is_internal: bool,
    /// Each partition in the topic.
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeTopicPartitionsResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    #[kafka(versions = "0+", default = -2147483648)]
    pub topic_authorized_operations: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partition index to start with.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 75,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct DescribeTopicPartitionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeTopicPartitionsResponseTopic>,
    /// The next topic and partition index to fetch details for.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub next_cursor: Option<Cursor>,
}
