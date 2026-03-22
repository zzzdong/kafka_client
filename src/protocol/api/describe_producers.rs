//! DescribeProducers API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 61

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeProducersRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 61, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeProducersRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeProducersRequestTopicRequest>,
}


/// DescribeProducersRequestTopicRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeProducersRequestTopicRequest {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partition_indexes: Vec<i32>,
}
/// DescribeProducersResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 61, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeProducersResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeProducersResponseTopicResponse>,
}


/// DescribeProducersResponseTopicResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeProducersResponseTopicResponse {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeProducersResponsePartitionResponse>,
}

/// DescribeProducersResponsePartitionResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeProducersResponsePartitionResponse {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub active_producers: Vec<DescribeProducersResponseProducerState>,
}

/// DescribeProducersResponseProducerState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeProducersResponseProducerState {
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i32,
    #[kafka(versions = "0+")]
    pub last_sequence: i32,
    #[kafka(versions = "0+")]
    pub last_timestamp: i64,
    #[kafka(versions = "0+")]
    pub coordinator_epoch: i32,
    #[kafka(versions = "0+")]
    pub current_txn_start_offset: i64,
}
