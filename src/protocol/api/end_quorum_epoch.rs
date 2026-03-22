//! EndQuorumEpoch API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 54

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// EndQuorumEpochRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 54, valid_versions = "0-1", flexible_versions = "1+")]
pub struct EndQuorumEpochRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<EndQuorumEpochRequestTopicData>,
    #[kafka(versions = "1+")]
    pub leader_endpoints: Vec<EndQuorumEpochRequestLeaderEndpoint>,
}


/// EndQuorumEpochRequestTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochRequestTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<EndQuorumEpochRequestPartitionData>,
}

/// EndQuorumEpochRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0")]
    pub preferred_successors: Vec<i32>,
    #[kafka(versions = "1+")]
    pub preferred_candidates: Vec<EndQuorumEpochRequestReplicaInfo>,
}

/// EndQuorumEpochRequestReplicaInfo
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochRequestReplicaInfo {
    #[kafka(versions = "1+")]
    pub candidate_id: i32,
    #[kafka(versions = "1+")]
    pub candidate_directory_id: Uuid,
}

/// EndQuorumEpochRequestLeaderEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochRequestLeaderEndpoint {
    #[kafka(versions = "1+")]
    pub name: String,
    #[kafka(versions = "1+")]
    pub host: String,
    #[kafka(versions = "1+")]
    pub port: i16,
}
/// EndQuorumEpochResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 54, valid_versions = "0-1", flexible_versions = "1+")]
pub struct EndQuorumEpochResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub topics: Vec<EndQuorumEpochResponseTopicData>,
    #[kafka(versions = "1+")]
    pub node_endpoints: Vec<EndQuorumEpochResponseNodeEndpoint>,
}


/// EndQuorumEpochResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochResponseTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<EndQuorumEpochResponsePartitionData>,
}

/// EndQuorumEpochResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

/// EndQuorumEpochResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct EndQuorumEpochResponseNodeEndpoint {
    #[kafka(versions = "1+")]
    pub node_id: i32,
    #[kafka(versions = "1+")]
    pub host: String,
    #[kafka(versions = "1+")]
    pub port: i16,
}
