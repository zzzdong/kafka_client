//! PushTelemetry API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 72

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// PushTelemetryRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 72, valid_versions = "0", flexible_versions = "0+")]
pub struct PushTelemetryRequest {
    #[kafka(versions = "0+")]
    pub client_instance_id: Uuid,
    #[kafka(versions = "0+")]
    pub subscription_id: i32,
    #[kafka(versions = "0+")]
    pub terminating: bool,
    #[kafka(versions = "0+")]
    pub compression_type: i8,
    #[kafka(versions = "0+")]
    pub metrics: Vec<u8>,
}

/// PushTelemetryResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 72, valid_versions = "0", flexible_versions = "0+")]
pub struct PushTelemetryResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

