//! Auto-generated from Kafka protocol
//! Message: AlterClientQuotasResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EntityData {
    /// The entity type.
    #[kafka(versions = "0+")]
    pub entity_type: String,
    /// The name of the entity, or null if the default.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub entity_name: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EntryData {
    /// The error code, or `0` if the quota alteration succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or `null` if the quota alteration succeeded.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The quota entity to alter.
    #[kafka(versions = "0+")]
    pub entity: Vec<EntityData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 49, msg_type = "response", valid_versions = "0-1", flexible_versions = "1+")]
pub struct AlterClientQuotasResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The quota configuration entries to alter.
    #[kafka(versions = "0+")]
    pub entries: Vec<EntryData>,
}

