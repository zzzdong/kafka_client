//! CreateDelegationToken API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 38

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// CreateDelegationTokenRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 38, valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateDelegationTokenRequest {
    #[kafka(versions = "3+")]
    pub owner_principal_type: String,
    #[kafka(versions = "3+")]
    pub owner_principal_name: String,
    #[kafka(versions = "0+")]
    pub renewers: Vec<CreateDelegationTokenRequestCreatableRenewers>,
    #[kafka(versions = "0+")]
    pub max_lifetime_ms: i64,
}


/// CreateDelegationTokenRequestCreatableRenewers
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateDelegationTokenRequestCreatableRenewers {
    #[kafka(versions = "0+")]
    pub principal_type: String,
    #[kafka(versions = "0+")]
    pub principal_name: String,
}
/// CreateDelegationTokenResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 38, valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateDelegationTokenResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub principal_type: String,
    #[kafka(versions = "0+")]
    pub principal_name: String,
    #[kafka(versions = "3+")]
    pub token_requester_principal_type: String,
    #[kafka(versions = "3+")]
    pub token_requester_principal_name: String,
    #[kafka(versions = "0+")]
    pub issue_timestamp_ms: i64,
    #[kafka(versions = "0+")]
    pub expiry_timestamp_ms: i64,
    #[kafka(versions = "0+")]
    pub max_timestamp_ms: i64,
    #[kafka(versions = "0+")]
    pub token_id: String,
    #[kafka(versions = "0+")]
    pub hmac: Vec<u8>,
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}

