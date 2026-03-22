//! ApiVersions API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 18

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ApiVersionsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 18, valid_versions = "0-4", flexible_versions = "3+")]
pub struct ApiVersionsRequest {
    #[kafka(versions = "3+")]
    pub client_software_name: String,
    #[kafka(versions = "3+")]
    pub client_software_version: String,
}

/// ApiVersionsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 18, valid_versions = "0-4", flexible_versions = "3+")]
pub struct ApiVersionsResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub api_keys: Vec<ApiVersionsResponseApiVersion>,
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "3+")]
    pub supported_features: Vec<ApiVersionsResponseSupportedFeatureKey>,
    #[kafka(versions = "3+")]
    pub finalized_features_epoch: i64,
    #[kafka(versions = "3+")]
    pub finalized_features: Vec<ApiVersionsResponseFinalizedFeatureKey>,
    #[kafka(versions = "3+")]
    pub zk_migration_ready: bool,
}


/// ApiVersionsResponseApiVersion
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ApiVersionsResponseApiVersion {
    #[kafka(versions = "0+")]
    pub api_key: i16,
    #[kafka(versions = "0+")]
    pub min_version: i16,
    #[kafka(versions = "0+")]
    pub max_version: i16,
}

/// ApiVersionsResponseSupportedFeatureKey
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ApiVersionsResponseSupportedFeatureKey {
    #[kafka(versions = "3+")]
    pub name: String,
    #[kafka(versions = "3+")]
    pub min_version: i16,
    #[kafka(versions = "3+")]
    pub max_version: i16,
}

/// ApiVersionsResponseFinalizedFeatureKey
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ApiVersionsResponseFinalizedFeatureKey {
    #[kafka(versions = "3+")]
    pub name: String,
    #[kafka(versions = "3+")]
    pub max_version_level: i16,
    #[kafka(versions = "3+")]
    pub min_version_level: i16,
}
