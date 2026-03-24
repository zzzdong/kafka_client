//! Auto-generated from Kafka protocol
//! Message: VoteResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The partition level error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The ID of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// True if the vote was granted and false otherwise.
    #[kafka(versions = "0+")]
    pub vote_granted: bool,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The results for each partition.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    #[kafka(versions = "1+", map_key)]
    pub node_id: i32,
    /// The node's hostname.
    #[kafka(versions = "1+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "1+")]
    pub port: u16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 52, msg_type = "response", valid_versions = "0-2", flexible_versions = "0+")]
pub struct VoteResponse {
    /// The top level error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The results for each topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
    /// Endpoints for all current-leaders enumerated in PartitionData.
    #[kafka(versions = "1+", tag = 0, tagged_versions = "1+")]
    pub node_endpoints: Vec<NodeEndpoint>,
}

