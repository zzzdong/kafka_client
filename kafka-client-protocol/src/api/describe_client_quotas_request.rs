//! Auto-generated from Kafka protocol
//! Message: DescribeClientQuotasRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ComponentData {
    /// The entity type that the filter component applies to.
    #[kafka(versions = "0+")]
    pub entity_type: String,
    /// How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
    #[kafka(versions = "0+")]
    pub match_type: i8,
    /// The string to match against, or null if unused for the match type.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub r#match: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 48, msg_type = "request", valid_versions = "0-1", flexible_versions = "1+")]
pub struct DescribeClientQuotasRequest {
    /// Filter components to apply to quota entities.
    #[kafka(versions = "0+")]
    pub components: Vec<ComponentData>,
    /// Whether the match is strict, i.e. should exclude entities with unspecified entity types.
    #[kafka(versions = "0+")]
    pub strict: bool,
}

