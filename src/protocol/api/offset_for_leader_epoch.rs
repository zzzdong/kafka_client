//! OffsetForLeaderEpoch API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 23

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// OffsetForLeaderEpochRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 23, valid_versions = "2-4", flexible_versions = "4+")]
pub struct OffsetForLeaderEpochRequest {
    #[kafka(versions = "3+")]
    pub replica_id: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetForLeaderEpochRequestOffsetForLeaderTopic>,
}


/// OffsetForLeaderEpochRequestOffsetForLeaderTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetForLeaderEpochRequestOffsetForLeaderTopic {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetForLeaderEpochRequestOffsetForLeaderPartition>,
}

/// OffsetForLeaderEpochRequestOffsetForLeaderPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetForLeaderEpochRequestOffsetForLeaderPartition {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "2+")]
    pub current_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}
/// OffsetForLeaderEpochResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 23, valid_versions = "2-4", flexible_versions = "4+")]
pub struct OffsetForLeaderEpochResponse {
    #[kafka(versions = "2+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetForLeaderEpochResponseOffsetForLeaderTopicResult>,
}


/// OffsetForLeaderEpochResponseOffsetForLeaderTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetForLeaderEpochResponseOffsetForLeaderTopicResult {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetForLeaderEpochResponseEpochEndOffset>,
}

/// OffsetForLeaderEpochResponseEpochEndOffset
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetForLeaderEpochResponseEpochEndOffset {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "1+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub end_offset: i64,
}
