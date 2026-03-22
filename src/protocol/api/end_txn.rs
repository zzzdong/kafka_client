//! EndTxn API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 26

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// EndTxnRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 26, valid_versions = "0-5", flexible_versions = "3+")]
pub struct EndTxnRequest {
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    #[kafka(versions = "0+")]
    pub committed: bool,
}

/// EndTxnResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 26, valid_versions = "0-5", flexible_versions = "3+")]
pub struct EndTxnResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "5+")]
    pub producer_id: i64,
    #[kafka(versions = "5+")]
    pub producer_epoch: i16,
}

