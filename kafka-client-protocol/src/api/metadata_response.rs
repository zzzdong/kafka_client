//! Auto-generated from Kafka protocol
//! Message: MetadataResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct MetadataResponseBroker {
    /// The broker ID.
    #[kafka(versions = "0+", map_key)]
    pub node_id: i32,
    /// The broker hostname.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The broker port.
    #[kafka(versions = "0+")]
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    #[kafka(versions = "1+", nullable_versions = "1+", default = None)]
    pub rack: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct MetadataResponsePartition {
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
    #[kafka(versions = "7+", default = -1)]
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    #[kafka(versions = "0+")]
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    #[kafka(versions = "0+")]
    pub isr_nodes: Vec<i32>,
    /// The set of offline replicas of this partition.
    #[kafka(versions = "5+")]
    pub offline_replicas: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct MetadataResponseTopic {
    /// The topic error, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
    #[kafka(versions = "0+", nullable_versions = "12+", map_key)]
    pub name: Option<String>,
    /// The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    /// True if the topic is internal.
    #[kafka(versions = "1+", default = false)]
    pub is_internal: bool,
    /// Each partition in the topic.
    #[kafka(versions = "0+")]
    pub partitions: Vec<MetadataResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    #[kafka(versions = "8+", default = -2147483648)]
    pub topic_authorized_operations: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 3,
    msg_type = "response",
    valid_versions = "0-13",
    flexible_versions = "9+"
)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "3+")]
    pub throttle_time_ms: i32,
    /// A list of brokers present in the cluster.
    #[kafka(versions = "0+")]
    pub brokers: Vec<MetadataResponseBroker>,
    /// The cluster ID that responding broker belongs to.
    #[kafka(versions = "2+", nullable_versions = "2+", default = None)]
    pub cluster_id: Option<String>,
    /// The ID of the controller broker.
    #[kafka(versions = "1+", default = -1)]
    pub controller_id: i32,
    /// Each topic in the response.
    #[kafka(versions = "0+")]
    pub topics: Vec<MetadataResponseTopic>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    #[kafka(versions = "8-10", default = -2147483648)]
    pub cluster_authorized_operations: i32,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "13+")]
    pub error_code: i16,
}
