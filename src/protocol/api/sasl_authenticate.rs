//! SaslAuthenticate API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 36

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// SaslAuthenticateRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 36, valid_versions = "0-2", flexible_versions = "2+")]
pub struct SaslAuthenticateRequest {
    #[kafka(versions = "0+")]
    pub auth_bytes: Vec<u8>,
}

/// SaslAuthenticateResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 36, valid_versions = "0-2", flexible_versions = "2+")]
pub struct SaslAuthenticateResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub auth_bytes: Vec<u8>,
    #[kafka(versions = "1+")]
    pub session_lifetime_ms: i64,
}

