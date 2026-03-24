//! Auto-generated from Kafka protocol
//! Message: ShareAcknowledgeResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The current leader of the partition.
    #[kafka(versions = "0+")]
    pub current_leader: LeaderIdAndEpoch,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ShareAcknowledgeTopicResponse {
    /// The unique topic ID.
    #[kafka(versions = "0+", map_key)]
    pub topic_id: Uuid,
    /// The topic partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    #[kafka(versions = "0+", map_key)]
    pub node_id: i32,
    /// The node's hostname.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "0+")]
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub rack: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 79, msg_type = "response", valid_versions = "1-2", flexible_versions = "0+")]
pub struct ShareAcknowledgeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The time in milliseconds for which the acquired records are locked.
    #[kafka(versions = "2+", nullable_versions = "2+")]
    pub acquisition_lock_timeout_ms: i32,
    /// The response topics.
    #[kafka(versions = "0+")]
    pub responses: Vec<ShareAcknowledgeTopicResponse>,
    /// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
    #[kafka(versions = "0+")]
    pub node_endpoints: Vec<NodeEndpoint>,
}

