//! Auto-generated from Kafka protocol
//! Message: ApiVersionsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ApiVersion {
    /// The API index.
    #[kafka(versions = "0+", map_key)]
    pub api_key: i16,
    /// The minimum supported version, inclusive.
    #[kafka(versions = "0+")]
    pub min_version: i16,
    /// The maximum supported version, inclusive.
    #[kafka(versions = "0+")]
    pub max_version: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct SupportedFeatureKey {
    /// The name of the feature.
    #[kafka(versions = "3+", map_key)]
    pub name: String,
    /// The minimum supported version for the feature.
    #[kafka(versions = "3+")]
    pub min_version: i16,
    /// The maximum supported version for the feature.
    #[kafka(versions = "3+")]
    pub max_version: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct FinalizedFeatureKey {
    /// The name of the feature.
    #[kafka(versions = "3+", map_key)]
    pub name: String,
    /// The cluster-wide finalized max version level for the feature.
    #[kafka(versions = "3+")]
    pub max_version_level: i16,
    /// The cluster-wide finalized min version level for the feature.
    #[kafka(versions = "3+")]
    pub min_version_level: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 18, msg_type = "response", valid_versions = "0-4", flexible_versions = "3+")]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The APIs supported by the broker.
    #[kafka(versions = "0+")]
    pub api_keys: Vec<ApiVersion>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    #[kafka(versions = "3+", nullable_versions = "3+", tag = 0, tagged_versions = "3+")]
    pub supported_features: Option<Vec<SupportedFeatureKey>>,
    /// The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    #[kafka(versions = "3+", nullable_versions = "3+", tag = 1, tagged_versions = "3+", default = -1)]
    pub finalized_features_epoch: i64,
    /// List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
    #[kafka(versions = "3+", nullable_versions = "3+", tag = 2, tagged_versions = "3+")]
    pub finalized_features: Option<Vec<FinalizedFeatureKey>>,
    /// Set by a KRaft controller if the required configurations for ZK migration are present.
    #[kafka(versions = "3+", nullable_versions = "3+", tag = 3, tagged_versions = "3+", default = false)]
    pub zk_migration_ready: bool,
}

