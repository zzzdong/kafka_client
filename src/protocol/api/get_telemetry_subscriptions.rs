//! GetTelemetrySubscriptions API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 71

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// GetTelemetrySubscriptionsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 71, valid_versions = "0", flexible_versions = "0+")]
pub struct GetTelemetrySubscriptionsRequest {
    #[kafka(versions = "0+")]
    pub client_instance_id: Uuid,
}

/// GetTelemetrySubscriptionsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 71, valid_versions = "0", flexible_versions = "0+")]
pub struct GetTelemetrySubscriptionsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub client_instance_id: Uuid,
    #[kafka(versions = "0+")]
    pub subscription_id: i32,
    #[kafka(versions = "0+")]
    pub accepted_compression_types: Vec<i8>,
    #[kafka(versions = "0+")]
    pub push_interval_ms: i32,
    #[kafka(versions = "0+")]
    pub telemetry_max_bytes: i32,
    #[kafka(versions = "0+")]
    pub delta_temporality: bool,
    #[kafka(versions = "0+")]
    pub requested_metrics: Vec<String>,
}

