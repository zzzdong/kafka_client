//! Auto-generated from Kafka protocol
//! Message: AlterClientQuotasRequest
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
pub struct OpData {
    /// The quota configuration key.
    #[kafka(versions = "0+")]
    pub key: String,
    /// The value to set, otherwise ignored if the value is to be removed.
    #[kafka(versions = "0+")]
    pub value: f64,
    /// Whether the quota configuration value should be removed, otherwise set.
    #[kafka(versions = "0+")]
    pub remove: bool,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EntryData {
    /// The quota entity to alter.
    #[kafka(versions = "0+")]
    pub entity: Vec<EntityData>,
    /// An individual quota configuration entry to alter.
    #[kafka(versions = "0+")]
    pub ops: Vec<OpData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 49, msg_type = "request", valid_versions = "0-1", flexible_versions = "1+")]
pub struct AlterClientQuotasRequest {
    /// The quota configuration entries to alter.
    #[kafka(versions = "0+")]
    pub entries: Vec<EntryData>,
    /// Whether the alteration should be validated, but not performed.
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}

