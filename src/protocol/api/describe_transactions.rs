//! DescribeTransactions API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 65

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeTransactionsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 65, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeTransactionsRequest {
    #[kafka(versions = "0+")]
    pub transactional_ids: Vec<String>,
}

/// DescribeTransactionsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 65, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeTransactionsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub transaction_states: Vec<DescribeTransactionsResponseTransactionState>,
}


/// DescribeTransactionsResponseTransactionState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTransactionsResponseTransactionState {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub transaction_state: String,
    #[kafka(versions = "0+")]
    pub transaction_timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub transaction_start_time_ms: i64,
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeTransactionsResponseTopicData>,
}

/// DescribeTransactionsResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTransactionsResponseTopicData {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
