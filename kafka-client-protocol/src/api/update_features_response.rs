//! Auto-generated from Kafka protocol
//! Message: UpdateFeaturesResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct UpdatableFeatureResult {
    /// The name of the finalized feature.
    #[kafka(versions = "0+", map_key)]
    pub feature: String,
    /// The feature update error code or `0` if the feature update succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The feature update error, or `null` if the feature update succeeded.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 57, msg_type = "response", valid_versions = "0-2", flexible_versions = "0+")]
pub struct UpdateFeaturesResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top-level error code, or `0` if there was no top-level error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or `null` if there was no top-level error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// Results for each feature update.
    #[kafka(versions = "0-1", nullable_versions = "0-1")]
    pub results: Option<Vec<UpdatableFeatureResult>>,
}

