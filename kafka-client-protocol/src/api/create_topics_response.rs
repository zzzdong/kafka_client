//! Auto-generated from Kafka protocol
//! Message: CreateTopicsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatableTopicConfigs {
    /// The configuration name.
    #[kafka(versions = "5+")]
    pub name: String,
    /// The configuration value.
    #[kafka(versions = "5+", nullable_versions = "5+")]
    pub value: Option<String>,
    /// True if the configuration is read-only.
    #[kafka(versions = "5+")]
    pub read_only: bool,
    /// The configuration source.
    #[kafka(versions = "5+", default = -1)]
    pub config_source: i8,
    /// True if this configuration is sensitive.
    #[kafka(versions = "5+")]
    pub is_sensitive: bool,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatableTopicResult {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The unique topic ID.
    #[kafka(versions = "7+")]
    pub topic_id: Uuid,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "1+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// Optional topic config error returned if configs are not returned in the response.
    #[kafka(versions = "5+", tag = 0, tagged_versions = "5+")]
    pub topic_config_error_code: i16,
    /// Number of partitions of the topic.
    #[kafka(versions = "5+", default = -1)]
    pub num_partitions: i32,
    /// Replication factor of the topic.
    #[kafka(versions = "5+", default = -1)]
    pub replication_factor: i16,
    /// Configuration of the topic.
    #[kafka(versions = "5+", nullable_versions = "5+")]
    pub configs: Option<Vec<CreatableTopicConfigs>>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 19,
    msg_type = "response",
    valid_versions = "2-7",
    flexible_versions = "5+"
)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "2+")]
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    #[kafka(versions = "0+")]
    pub topics: Vec<CreatableTopicResult>,
}
