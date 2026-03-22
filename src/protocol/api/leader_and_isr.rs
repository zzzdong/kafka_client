//! LeaderAndIsr API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 4

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// LeaderAndIsrRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 4, valid_versions = "none")]
pub struct LeaderAndIsrRequest {
}

/// LeaderAndIsrResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 4, valid_versions = "none")]
pub struct LeaderAndIsrResponse {
}

