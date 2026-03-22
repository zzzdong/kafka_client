//! BrokerRegistration API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 62

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// BrokerRegistrationRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 62, valid_versions = "0-4", flexible_versions = "0+")]
pub struct BrokerRegistrationRequest {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub incarnation_id: Uuid,
    #[kafka(versions = "0+")]
    pub listeners: Vec<BrokerRegistrationRequestListener>,
    #[kafka(versions = "0+")]
    pub features: Vec<BrokerRegistrationRequestFeature>,
    #[kafka(versions = "0+")]
    pub rack: String,
    #[kafka(versions = "1+")]
    pub is_migrating_zk_broker: bool,
    #[kafka(versions = "2+")]
    pub log_dirs: Vec<Uuid>,
    #[kafka(versions = "3+")]
    pub previous_broker_epoch: i64,
}


/// BrokerRegistrationRequestListener
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BrokerRegistrationRequestListener {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i16,
    #[kafka(versions = "0+")]
    pub security_protocol: i16,
}

/// BrokerRegistrationRequestFeature
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct BrokerRegistrationRequestFeature {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub min_supported_version: i16,
    #[kafka(versions = "0+")]
    pub max_supported_version: i16,
}
/// BrokerRegistrationResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 62, valid_versions = "0-4", flexible_versions = "0+")]
pub struct BrokerRegistrationResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub broker_epoch: i64,
}

