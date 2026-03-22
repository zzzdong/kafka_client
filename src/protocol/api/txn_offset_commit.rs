//! TxnOffsetCommit API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 28

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// TxnOffsetCommitRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 28, valid_versions = "0-5", flexible_versions = "3+")]
pub struct TxnOffsetCommitRequest {
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    #[kafka(versions = "3+")]
    pub generation_id: i32,
    #[kafka(versions = "3+")]
    pub member_id: String,
    #[kafka(versions = "3+")]
    pub group_instance_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<TxnOffsetCommitRequestTxnOffsetCommitRequestTopic>,
}


/// TxnOffsetCommitRequestTxnOffsetCommitRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct TxnOffsetCommitRequestTxnOffsetCommitRequestTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<TxnOffsetCommitRequestTxnOffsetCommitRequestPartition>,
}

/// TxnOffsetCommitRequestTxnOffsetCommitRequestPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct TxnOffsetCommitRequestTxnOffsetCommitRequestPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub committed_offset: i64,
    #[kafka(versions = "2+")]
    pub committed_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub committed_metadata: String,
}
/// TxnOffsetCommitResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 28, valid_versions = "0-5", flexible_versions = "3+")]
pub struct TxnOffsetCommitResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<TxnOffsetCommitResponseTxnOffsetCommitResponseTopic>,
}


/// TxnOffsetCommitResponseTxnOffsetCommitResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct TxnOffsetCommitResponseTxnOffsetCommitResponseTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<TxnOffsetCommitResponseTxnOffsetCommitResponsePartition>,
}

/// TxnOffsetCommitResponseTxnOffsetCommitResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct TxnOffsetCommitResponseTxnOffsetCommitResponsePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
