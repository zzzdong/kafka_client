//! FindCoordinator API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 10

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// FindCoordinatorRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 10, valid_versions = "0-6", flexible_versions = "3+")]
pub struct FindCoordinatorRequest {
    #[kafka(versions = "0-3")]
    pub key: String,
    #[kafka(versions = "1+")]
    pub key_type: i8,
    #[kafka(versions = "4+")]
    pub coordinator_keys: Vec<String>,
}

/// FindCoordinatorResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 10, valid_versions = "0-6", flexible_versions = "3+")]
pub struct FindCoordinatorResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0-3")]
    pub error_code: i16,
    #[kafka(versions = "1-3")]
    pub error_message: String,
    #[kafka(versions = "0-3")]
    pub node_id: i32,
    #[kafka(versions = "0-3")]
    pub host: String,
    #[kafka(versions = "0-3")]
    pub port: i32,
    #[kafka(versions = "4+")]
    pub coordinators: Vec<FindCoordinatorResponseCoordinator>,
}


/// FindCoordinatorResponseCoordinator
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FindCoordinatorResponseCoordinator {
    #[kafka(versions = "4+")]
    pub key: String,
    #[kafka(versions = "4+")]
    pub node_id: i32,
    #[kafka(versions = "4+")]
    pub host: String,
    #[kafka(versions = "4+")]
    pub port: i32,
    #[kafka(versions = "4+")]
    pub error_code: i16,
    #[kafka(versions = "4+")]
    pub error_message: String,
}
