//! Auto-generated from Kafka protocol
//! Message: IncrementalAlterConfigsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterableConfig {
    /// The configuration key name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The type (Set, Delete, Append, Subtract) of operation.
    #[kafka(versions = "0+", map_key)]
    pub config_operation: i8,
    /// The value to set for the configuration key.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub value: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterConfigsResource {
    /// The resource type.
    #[kafka(versions = "0+", map_key)]
    pub resource_type: i8,
    /// The resource name.
    #[kafka(versions = "0+", map_key)]
    pub resource_name: String,
    /// The configurations.
    #[kafka(versions = "0+")]
    pub configs: Vec<AlterableConfig>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 44,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "1+"
)]
pub struct IncrementalAlterConfigsRequest {
    /// The incremental updates for each resource.
    #[kafka(versions = "0+")]
    pub resources: Vec<AlterConfigsResource>,
    /// True if we should validate the request, but not change the configurations.
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}
