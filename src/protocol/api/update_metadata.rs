//! UpdateMetadata API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 6

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// UpdateMetadataRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 6, valid_versions = "none")]
pub struct UpdateMetadataRequest {
}

/// UpdateMetadataResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 6, valid_versions = "none")]
pub struct UpdateMetadataResponse {
}

