//! Auto-generated from Kafka protocol
//! Message: DescribeConfigsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeConfigsResource {
    /// The resource type.
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    /// The resource name.
    #[kafka(versions = "0+")]
    pub resource_name: String,
    /// The configuration keys to list, or null to list all configuration keys.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub configuration_keys: Option<Vec<String>>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 32, msg_type = "request", valid_versions = "1-4", flexible_versions = "4+")]
pub struct DescribeConfigsRequest {
    /// The resources whose configurations we want to describe.
    #[kafka(versions = "0+")]
    pub resources: Vec<DescribeConfigsResource>,
    /// True if we should include all synonyms.
    #[kafka(versions = "1+", default = false)]
    pub include_synonyms: bool,
    /// True if we should include configuration documentation.
    #[kafka(versions = "3+", default = false)]
    pub include_documentation: bool,
}

