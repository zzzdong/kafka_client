//! SaslHandshake API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 17

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// SaslHandshakeRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 17, valid_versions = "0-1", flexible_versions = "none")]
pub struct SaslHandshakeRequest {
    #[kafka(versions = "0+")]
    pub mechanism: String,
}

/// SaslHandshakeResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 17, valid_versions = "0-1", flexible_versions = "none")]
pub struct SaslHandshakeResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub mechanisms: Vec<String>,
}

