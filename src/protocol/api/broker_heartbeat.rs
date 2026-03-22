//! BrokerHeartbeat API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 63

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// BrokerHeartbeatRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 63, valid_versions = "0-1", flexible_versions = "0+")]
pub struct BrokerHeartbeatRequest {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    #[kafka(versions = "0+")]
    pub broker_epoch: i64,
    #[kafka(versions = "0+")]
    pub current_metadata_offset: i64,
    #[kafka(versions = "0+")]
    pub want_fence: bool,
    #[kafka(versions = "0+")]
    pub want_shut_down: bool,
    #[kafka(versions = "1+")]
    pub offline_log_dirs: Vec<Uuid>,
}

/// BrokerHeartbeatResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 63, valid_versions = "0-1", flexible_versions = "0+")]
pub struct BrokerHeartbeatResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub is_caught_up: bool,
    #[kafka(versions = "0+")]
    pub is_fenced: bool,
    #[kafka(versions = "0+")]
    pub should_shut_down: bool,
}

