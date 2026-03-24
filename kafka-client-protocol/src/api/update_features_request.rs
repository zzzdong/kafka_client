//! Auto-generated from Kafka protocol
//! Message: UpdateFeaturesRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct FeatureUpdateKey {
    /// The name of the finalized feature to be updated.
    #[kafka(versions = "0+", map_key)]
    pub feature: String,
    /// The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
    #[kafka(versions = "0+")]
    pub max_version_level: i16,
    /// DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
    #[kafka(versions = "0")]
    pub allow_downgrade: bool,
    /// Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
    #[kafka(versions = "1+", default = 1)]
    pub upgrade_type: i8,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 57, msg_type = "request", valid_versions = "0-2", flexible_versions = "0+")]
pub struct UpdateFeaturesRequest {
    /// How long to wait in milliseconds before timing out the request.
    #[kafka(versions = "0+", default = 60000)]
    pub timeout_ms: i32,
    /// The list of updates to finalized features.
    #[kafka(versions = "0+")]
    pub feature_updates: Vec<FeatureUpdateKey>,
    /// True if we should validate the request, but not perform the upgrade or downgrade.
    #[kafka(versions = "1+", default = false)]
    pub validate_only: bool,
}

