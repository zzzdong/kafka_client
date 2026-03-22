//! ControlledShutdown API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 7

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ControlledShutdownRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 7, valid_versions = "none")]
pub struct ControlledShutdownRequest {
}

/// ControlledShutdownResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 7, valid_versions = "none")]
pub struct ControlledShutdownResponse {
}

