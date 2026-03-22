//! DescribeQuorum API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 55

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeQuorumRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 55, valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeQuorumRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeQuorumRequestTopicData>,
}


/// DescribeQuorumRequestTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumRequestTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeQuorumRequestPartitionData>,
}

/// DescribeQuorumRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}
/// DescribeQuorumResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 55, valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeQuorumResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "2+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeQuorumResponseTopicData>,
    #[kafka(versions = "2+")]
    pub nodes: Vec<DescribeQuorumResponseNode>,
}


/// DescribeQuorumResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumResponseTopicData {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeQuorumResponsePartitionData>,
}

/// DescribeQuorumResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "2+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub high_watermark: i64,
    #[kafka(versions = "0+")]
    pub current_voters: Vec<ReplicaState>,
    #[kafka(versions = "0+")]
    pub observers: Vec<ReplicaState>,
}

/// DescribeQuorumResponseNode
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumResponseNode {
    #[kafka(versions = "2+")]
    pub node_id: i32,
    #[kafka(versions = "2+")]
    pub listeners: Vec<DescribeQuorumResponseListener>,
}

/// DescribeQuorumResponseListener
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumResponseListener {
    #[kafka(versions = "2+")]
    pub name: String,
    #[kafka(versions = "2+")]
    pub host: String,
    #[kafka(versions = "2+")]
    pub port: i16,
}

/// DescribeQuorumResponseReplicaState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeQuorumResponseReplicaState {
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    #[kafka(versions = "2+")]
    pub replica_directory_id: Uuid,
    #[kafka(versions = "0+")]
    pub log_end_offset: i64,
    #[kafka(versions = "1+")]
    pub last_fetch_timestamp: i64,
    #[kafka(versions = "1+")]
    pub last_caught_up_timestamp: i64,
}
