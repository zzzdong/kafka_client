//! Auto-generated from Kafka protocol
//! Message: DescribeQuorumResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReplicaState {
    /// The ID of the replica.
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    /// The replica directory ID of the replica.
    #[kafka(versions = "2+")]
    pub replica_directory_id: Uuid,
    /// The last known log end offset of the follower or -1 if it is unknown.
    #[kafka(versions = "0+")]
    pub log_end_offset: i64,
    /// The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter.
    #[kafka(versions = "1+", nullable_versions = "1+", default = -1)]
    pub last_fetch_timestamp: i64,
    /// The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter.
    #[kafka(versions = "1+", nullable_versions = "1+", default = -1)]
    pub last_caught_up_timestamp: i64,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The partition error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "2+", nullable_versions = "2+")]
    pub error_message: Option<String>,
    /// The ID of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// The high water mark.
    #[kafka(versions = "0+")]
    pub high_watermark: i64,
    /// The current voters of the partition.
    #[kafka(versions = "0+")]
    pub current_voters: Vec<ReplicaState>,
    /// The observers of the partition.
    #[kafka(versions = "0+")]
    pub observers: Vec<ReplicaState>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partition data.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    #[kafka(versions = "2+", map_key)]
    pub name: String,
    /// The hostname.
    #[kafka(versions = "2+")]
    pub host: String,
    /// The port.
    #[kafka(versions = "2+")]
    pub port: u16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Node {
    /// The ID of the associated node.
    #[kafka(versions = "2+", map_key)]
    pub node_id: i32,
    /// The listeners of this controller.
    #[kafka(versions = "2+")]
    pub listeners: Vec<Listener>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 55, msg_type = "response", valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeQuorumResponse {
    /// The top level error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "2+", nullable_versions = "2+")]
    pub error_message: Option<String>,
    /// The response from the describe quorum API.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
    /// The nodes in the quorum.
    #[kafka(versions = "2+")]
    pub nodes: Vec<Node>,
}

