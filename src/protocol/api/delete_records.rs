//! DeleteRecords API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 21

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DeleteRecordsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 21, valid_versions = "0-2", flexible_versions = "2+")]
pub struct DeleteRecordsRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteRecordsRequestDeleteRecordsTopic>,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
}


/// DeleteRecordsRequestDeleteRecordsTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteRecordsRequestDeleteRecordsTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DeleteRecordsRequestDeleteRecordsPartition>,
}

/// DeleteRecordsRequestDeleteRecordsPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteRecordsRequestDeleteRecordsPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub offset: i64,
}
/// DeleteRecordsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 21, valid_versions = "0-2", flexible_versions = "2+")]
pub struct DeleteRecordsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteRecordsResponseDeleteRecordsTopicResult>,
}


/// DeleteRecordsResponseDeleteRecordsTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteRecordsResponseDeleteRecordsTopicResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DeleteRecordsResponseDeleteRecordsPartitionResult>,
}

/// DeleteRecordsResponseDeleteRecordsPartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteRecordsResponseDeleteRecordsPartitionResult {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub low_watermark: i64,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
