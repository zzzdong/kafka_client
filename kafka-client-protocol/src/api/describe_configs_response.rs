//! Auto-generated from Kafka protocol
//! Message: DescribeConfigsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeConfigsSynonym {
    /// The synonym name.
    #[kafka(versions = "1+")]
    pub name: String,
    /// The synonym value.
    #[kafka(versions = "1+", nullable_versions = "0+")]
    pub value: Option<String>,
    /// The synonym source.
    #[kafka(versions = "1+")]
    pub source: i8,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeConfigsResourceResult {
    /// The configuration name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The configuration value.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub value: Option<String>,
    /// True if the configuration is read-only.
    #[kafka(versions = "0+")]
    pub read_only: bool,
    /// The configuration source.
    #[kafka(versions = "1+", nullable_versions = "1+", default = -1)]
    pub config_source: i8,
    /// True if this configuration is sensitive.
    #[kafka(versions = "0+")]
    pub is_sensitive: bool,
    /// The synonyms for this configuration key.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub synonyms: Option<Vec<DescribeConfigsSynonym>>,
    /// The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
    #[kafka(versions = "3+", nullable_versions = "3+", default = 0)]
    pub config_type: i8,
    /// The configuration documentation.
    #[kafka(versions = "3+", nullable_versions = "0+")]
    pub documentation: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeConfigsResult {
    /// The error code, or 0 if we were able to successfully describe the configurations.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if we were able to successfully describe the configurations.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The resource type.
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    /// The resource name.
    #[kafka(versions = "0+")]
    pub resource_name: String,
    /// Each listed configuration.
    #[kafka(versions = "0+")]
    pub configs: Vec<DescribeConfigsResourceResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 32,
    msg_type = "response",
    valid_versions = "1-4",
    flexible_versions = "4+"
)]
pub struct DescribeConfigsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The results for each resource.
    #[kafka(versions = "0+")]
    pub results: Vec<DescribeConfigsResult>,
}
