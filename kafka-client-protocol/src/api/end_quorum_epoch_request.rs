//! Auto-generated from Kafka protocol
//! Message: EndQuorumEpochRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReplicaInfo {
    /// The ID of the candidate replica.
    #[kafka(versions = "1+")]
    pub candidate_id: i32,
    /// The directory ID of the candidate replica.
    #[kafka(versions = "1+")]
    pub candidate_directory_id: Uuid,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The current leader ID that is resigning.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The current epoch.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// A sorted list of preferred successors to start the election.
    #[kafka(versions = "0")]
    pub preferred_successors: Vec<i32>,
    /// A sorted list of preferred candidates to start the election.
    #[kafka(versions = "1+")]
    pub preferred_candidates: Vec<ReplicaInfo>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct LeaderEndpoint {
    /// The name of the endpoint.
    #[kafka(versions = "1+", map_key)]
    pub name: String,
    /// The node's hostname.
    #[kafka(versions = "1+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "1+")]
    pub port: u16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 54,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "1+"
)]
pub struct EndQuorumEpochRequest {
    /// The cluster id.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub cluster_id: Option<String>,
    /// The topics.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
    /// Endpoints for the leader.
    #[kafka(versions = "1+")]
    pub leader_endpoints: Vec<LeaderEndpoint>,
}
