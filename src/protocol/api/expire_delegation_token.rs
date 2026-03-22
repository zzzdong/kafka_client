//! ExpireDelegationToken API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 40

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ExpireDelegationTokenRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 40, valid_versions = "1-2", flexible_versions = "2+")]
pub struct ExpireDelegationTokenRequest {
    #[kafka(versions = "0+")]
    pub hmac: Vec<u8>,
    #[kafka(versions = "0+")]
    pub expiry_time_period_ms: i64,
}

/// ExpireDelegationTokenResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 40, valid_versions = "1-2", flexible_versions = "2+")]
pub struct ExpireDelegationTokenResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub expiry_timestamp_ms: i64,
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}

