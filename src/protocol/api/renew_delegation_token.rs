//! RenewDelegationToken API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 39

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// RenewDelegationTokenRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 39, valid_versions = "1-2", flexible_versions = "2+")]
pub struct RenewDelegationTokenRequest {
    #[kafka(versions = "0+")]
    pub hmac: Vec<u8>,
    #[kafka(versions = "0+")]
    pub renew_period_ms: i64,
}

/// RenewDelegationTokenResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 39, valid_versions = "1-2", flexible_versions = "2+")]
pub struct RenewDelegationTokenResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub expiry_timestamp_ms: i64,
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}

