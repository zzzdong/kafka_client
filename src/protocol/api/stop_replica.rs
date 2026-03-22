//! StopReplica API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 5

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// StopReplicaRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 5, valid_versions = "none")]
pub struct StopReplicaRequest {
}

/// StopReplicaResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 5, valid_versions = "none")]
pub struct StopReplicaResponse {
}

