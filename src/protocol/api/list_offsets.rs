//! ListOffsets API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 2

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ListOffsetsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 2, valid_versions = "1-11", flexible_versions = "6+")]
pub struct ListOffsetsRequest {
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    #[kafka(versions = "2+")]
    pub isolation_level: i8,
    #[kafka(versions = "0+")]
    pub topics: Vec<ListOffsetsRequestListOffsetsTopic>,
    #[kafka(versions = "10+")]
    pub timeout_ms: i32,
}


/// ListOffsetsRequestListOffsetsTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListOffsetsRequestListOffsetsTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ListOffsetsRequestListOffsetsPartition>,
}

/// ListOffsetsRequestListOffsetsPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListOffsetsRequestListOffsetsPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "4+")]
    pub current_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub timestamp: i64,
}
/// ListOffsetsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 2, valid_versions = "1-11", flexible_versions = "6+")]
pub struct ListOffsetsResponse {
    #[kafka(versions = "2+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<ListOffsetsResponseListOffsetsTopicResponse>,
}


/// ListOffsetsResponseListOffsetsTopicResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListOffsetsResponseListOffsetsTopicResponse {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ListOffsetsResponseListOffsetsPartitionResponse>,
}

/// ListOffsetsResponseListOffsetsPartitionResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListOffsetsResponseListOffsetsPartitionResponse {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "1+")]
    pub timestamp: i64,
    #[kafka(versions = "1+")]
    pub offset: i64,
    #[kafka(versions = "4+")]
    pub leader_epoch: i32,
}
