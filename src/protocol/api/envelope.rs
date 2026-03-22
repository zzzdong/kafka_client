//! Envelope API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 58

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// EnvelopeRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 58, valid_versions = "0", flexible_versions = "0+")]
pub struct EnvelopeRequest {
    #[kafka(versions = "0+")]
    pub request_data: Vec<u8>,
    #[kafka(versions = "0+")]
    pub request_principal: Vec<u8>,
    #[kafka(versions = "0+")]
    pub client_host_address: Vec<u8>,
}

/// EnvelopeResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 58, valid_versions = "0", flexible_versions = "0+")]
pub struct EnvelopeResponse {
    #[kafka(versions = "0+")]
    pub response_data: Vec<u8>,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

