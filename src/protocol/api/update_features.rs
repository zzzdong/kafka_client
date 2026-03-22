//! UpdateFeatures API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 57

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// UpdateFeaturesRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 57, valid_versions = "0-2", flexible_versions = "0+")]
pub struct UpdateFeaturesRequest {
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub feature_updates: Vec<UpdateFeaturesRequestFeatureUpdateKey>,
    #[kafka(versions = "1+")]
    pub validate_only: bool,
}


/// UpdateFeaturesRequestFeatureUpdateKey
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct UpdateFeaturesRequestFeatureUpdateKey {
    #[kafka(versions = "0+")]
    pub feature: String,
    #[kafka(versions = "0+")]
    pub max_version_level: i16,
    #[kafka(versions = "0")]
    pub allow_downgrade: bool,
    #[kafka(versions = "1+")]
    pub upgrade_type: i8,
}
/// UpdateFeaturesResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 57, valid_versions = "0-2", flexible_versions = "0+")]
pub struct UpdateFeaturesResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0-1")]
    pub results: Vec<UpdateFeaturesResponseUpdatableFeatureResult>,
}


/// UpdateFeaturesResponseUpdatableFeatureResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct UpdateFeaturesResponseUpdatableFeatureResult {
    #[kafka(versions = "0+")]
    pub feature: String,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
