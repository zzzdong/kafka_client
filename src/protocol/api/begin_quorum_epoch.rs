//! BeginQuorumEpoch API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 53

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// BeginQuorumEpochRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 53, valid_versions = "0-1", flexible_versions = "1+")]
pub struct BeginQuorumEpochRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "1+")]
    pub voter_id: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<BeginQuorumEpochRequestTopicData>,
    #[kafka(versions = "1+")]
    pub leader_endpoints: Vec<BeginQuorumEpochRequestLeaderEndpoint>,
}


/// BeginQuorumEpochRequestTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BeginQuorumEpochRequestTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<BeginQuorumEpochRequestPartitionData>,
}

/// BeginQuorumEpochRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BeginQuorumEpochRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "1+")]
    pub voter_directory_id: Uuid,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

/// BeginQuorumEpochRequestLeaderEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BeginQuorumEpochRequestLeaderEndpoint {
    #[kafka(versions = "1+")]
    pub name: String,
    #[kafka(versions = "1+")]
    pub host: String,
    #[kafka(versions = "1+")]
    pub port: i16,
}
/// BeginQuorumEpochResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 53, valid_versions = "0-1", flexible_versions = "1+")]
pub struct BeginQuorumEpochResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub topics: Vec<BeginQuorumEpochResponseTopicData>,
    #[kafka(versions = "1+")]
    pub node_endpoints: Vec<BeginQuorumEpochResponseNodeEndpoint>,
}


/// BeginQuorumEpochResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BeginQuorumEpochResponseTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<BeginQuorumEpochResponsePartitionData>,
}

/// BeginQuorumEpochResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BeginQuorumEpochResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

/// BeginQuorumEpochResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BeginQuorumEpochResponseNodeEndpoint {
    #[kafka(versions = "1+")]
    pub node_id: i32,
    #[kafka(versions = "1+")]
    pub host: String,
    #[kafka(versions = "1+")]
    pub port: i16,
}
