//! Auto-generated from Kafka protocol
//! Message: DescribeClientQuotasResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EntityData {
    /// The entity type.
    #[kafka(versions = "0+")]
    pub entity_type: String,
    /// The entity name, or null if the default.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub entity_name: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ValueData {
    /// The quota configuration key.
    #[kafka(versions = "0+")]
    pub key: String,
    /// The quota configuration value.
    #[kafka(versions = "0+")]
    pub value: f64,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EntryData {
    /// The quota entity description.
    #[kafka(versions = "0+")]
    pub entity: Vec<EntityData>,
    /// The quota values for the entity.
    #[kafka(versions = "0+")]
    pub values: Vec<ValueData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 48, msg_type = "response", valid_versions = "0-1", flexible_versions = "1+")]
pub struct DescribeClientQuotasResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or `0` if the quota description succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or `null` if the quota description succeeded.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// A result entry.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub entries: Option<Vec<EntryData>>,
}

