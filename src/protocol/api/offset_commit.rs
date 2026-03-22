//! OffsetCommit API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 8

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// OffsetCommitRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 8, valid_versions = "2-10", flexible_versions = "8+")]
pub struct OffsetCommitRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "1+")]
    pub generation_id_or_member_epoch: i32,
    #[kafka(versions = "1+")]
    pub member_id: String,
    #[kafka(versions = "7+")]
    pub group_instance_id: String,
    #[kafka(versions = "2-4")]
    pub retention_time_ms: i64,
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetCommitRequestOffsetCommitRequestTopic>,
}


/// OffsetCommitRequestOffsetCommitRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetCommitRequestOffsetCommitRequestTopic {
    #[kafka(versions = "0-9")]
    pub name: String,
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetCommitRequestOffsetCommitRequestPartition>,
}

/// OffsetCommitRequestOffsetCommitRequestPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetCommitRequestOffsetCommitRequestPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub committed_offset: i64,
    #[kafka(versions = "6+")]
    pub committed_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub committed_metadata: String,
}
/// OffsetCommitResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 8, valid_versions = "2-10", flexible_versions = "8+")]
pub struct OffsetCommitResponse {
    #[kafka(versions = "3+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetCommitResponseOffsetCommitResponseTopic>,
}


/// OffsetCommitResponseOffsetCommitResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetCommitResponseOffsetCommitResponseTopic {
    #[kafka(versions = "0-9")]
    pub name: String,
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetCommitResponseOffsetCommitResponsePartition>,
}

/// OffsetCommitResponseOffsetCommitResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetCommitResponseOffsetCommitResponsePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
