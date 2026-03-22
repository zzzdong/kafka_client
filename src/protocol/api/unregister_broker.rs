//! UnregisterBroker API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 64

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// UnregisterBrokerRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 64, valid_versions = "0", flexible_versions = "0+")]
pub struct UnregisterBrokerRequest {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
}

/// UnregisterBrokerResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 64, valid_versions = "0", flexible_versions = "0+")]
pub struct UnregisterBrokerResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}

