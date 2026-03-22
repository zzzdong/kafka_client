//! Heartbeat API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 12

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// HeartbeatRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 12, valid_versions = "0-4", flexible_versions = "4+")]
pub struct HeartbeatRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub generation_id: i32,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "3+")]
    pub group_instance_id: String,
}

/// HeartbeatResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 12, valid_versions = "0-4", flexible_versions = "4+")]
pub struct HeartbeatResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

