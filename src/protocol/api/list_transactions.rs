//! ListTransactions API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 66

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ListTransactionsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 66, valid_versions = "0-2", flexible_versions = "0+")]
pub struct ListTransactionsRequest {
    #[kafka(versions = "0+")]
    pub state_filters: Vec<String>,
    #[kafka(versions = "0+")]
    pub producer_id_filters: Vec<i64>,
    #[kafka(versions = "1+")]
    pub duration_filter: i64,
    #[kafka(versions = "2+")]
    pub transactional_id_pattern: String,
}

/// ListTransactionsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 66, valid_versions = "0-2", flexible_versions = "0+")]
pub struct ListTransactionsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub unknown_state_filters: Vec<String>,
    #[kafka(versions = "0+")]
    pub transaction_states: Vec<ListTransactionsResponseTransactionState>,
}


/// ListTransactionsResponseTransactionState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListTransactionsResponseTransactionState {
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub transaction_state: String,
}
