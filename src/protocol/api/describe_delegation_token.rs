//! DescribeDelegationToken API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 41

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeDelegationTokenRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 41, valid_versions = "1-3", flexible_versions = "2+")]
pub struct DescribeDelegationTokenRequest {
    #[kafka(versions = "0+")]
    pub owners: Vec<DescribeDelegationTokenRequestDescribeDelegationTokenOwner>,
}


/// DescribeDelegationTokenRequestDescribeDelegationTokenOwner
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeDelegationTokenRequestDescribeDelegationTokenOwner {
    #[kafka(versions = "0+")]
    pub principal_type: String,
    #[kafka(versions = "0+")]
    pub principal_name: String,
}
/// DescribeDelegationTokenResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 41, valid_versions = "1-3", flexible_versions = "2+")]
pub struct DescribeDelegationTokenResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub tokens: Vec<DescribeDelegationTokenResponseDescribedDelegationToken>,
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}


/// DescribeDelegationTokenResponseDescribedDelegationToken
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeDelegationTokenResponseDescribedDelegationToken {
    #[kafka(versions = "0+")]
    pub principal_type: String,
    #[kafka(versions = "0+")]
    pub principal_name: String,
    #[kafka(versions = "3+")]
    pub token_requester_principal_type: String,
    #[kafka(versions = "3+")]
    pub token_requester_principal_name: String,
    #[kafka(versions = "0+")]
    pub issue_timestamp: i64,
    #[kafka(versions = "0+")]
    pub expiry_timestamp: i64,
    #[kafka(versions = "0+")]
    pub max_timestamp: i64,
    #[kafka(versions = "0+")]
    pub token_id: String,
    #[kafka(versions = "0+")]
    pub hmac: Vec<u8>,
    #[kafka(versions = "0+")]
    pub renewers: Vec<DescribeDelegationTokenResponseDescribedDelegationTokenRenewer>,
}

/// DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeDelegationTokenResponseDescribedDelegationTokenRenewer {
    #[kafka(versions = "0+")]
    pub principal_type: String,
    #[kafka(versions = "0+")]
    pub principal_name: String,
}
