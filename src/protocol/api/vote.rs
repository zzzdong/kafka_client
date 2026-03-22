//! Vote API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 52

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// VoteRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 52, valid_versions = "0-2", flexible_versions = "0+")]
pub struct VoteRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "1+")]
    pub voter_id: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<VoteRequestTopicData>,
}


/// VoteRequestTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct VoteRequestTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<VoteRequestPartitionData>,
}

/// VoteRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct VoteRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub replica_epoch: i32,
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    #[kafka(versions = "1+")]
    pub replica_directory_id: Uuid,
    #[kafka(versions = "1+")]
    pub voter_directory_id: Uuid,
    #[kafka(versions = "0+")]
    pub last_offset_epoch: i32,
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    #[kafka(versions = "2+")]
    pub pre_vote: bool,
}
/// VoteResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 52, valid_versions = "0-2", flexible_versions = "0+")]
pub struct VoteResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub topics: Vec<VoteResponseTopicData>,
    #[kafka(versions = "1+")]
    pub node_endpoints: Vec<VoteResponseNodeEndpoint>,
}


/// VoteResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct VoteResponseTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<VoteResponsePartitionData>,
}

/// VoteResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct VoteResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub vote_granted: bool,
}

/// VoteResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct VoteResponseNodeEndpoint {
    #[kafka(versions = "1+")]
    pub node_id: i32,
    #[kafka(versions = "1+")]
    pub host: String,
    #[kafka(versions = "1+")]
    pub port: i16,
}
