//! ControllerRegistration API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 70

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ControllerRegistrationRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 70, valid_versions = "0", flexible_versions = "0+")]
pub struct ControllerRegistrationRequest {
    #[kafka(versions = "0+")]
    pub controller_id: i32,
    #[kafka(versions = "0+")]
    pub incarnation_id: Uuid,
    #[kafka(versions = "0+")]
    pub zk_migration_ready: bool,
    #[kafka(versions = "0+")]
    pub listeners: Vec<ControllerRegistrationRequestListener>,
    #[kafka(versions = "0+")]
    pub features: Vec<ControllerRegistrationRequestFeature>,
}


/// ControllerRegistrationRequestListener
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ControllerRegistrationRequestListener {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i16,
    #[kafka(versions = "0+")]
    pub security_protocol: i16,
}

/// ControllerRegistrationRequestFeature
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ControllerRegistrationRequestFeature {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub min_supported_version: i16,
    #[kafka(versions = "0+")]
    pub max_supported_version: i16,
}
/// ControllerRegistrationResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 70, valid_versions = "0", flexible_versions = "0+")]
pub struct ControllerRegistrationResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}

