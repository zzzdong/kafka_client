//! Auto-generated from Kafka protocol
//! Message: DescribeAclsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 29, msg_type = "request", valid_versions = "1-3", flexible_versions = "2+")]
pub struct DescribeAclsRequest {
    /// The resource type.
    #[kafka(versions = "0+")]
    pub resource_type_filter: i8,
    /// The resource name, or null to match any resource name.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub resource_name_filter: Option<String>,
    /// The resource pattern to match.
    #[kafka(versions = "1+", default = 3)]
    pub pattern_type_filter: i8,
    /// The principal to match, or null to match any principal.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub principal_filter: Option<String>,
    /// The host to match, or null to match any host.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub host_filter: Option<String>,
    /// The operation to match.
    #[kafka(versions = "0+")]
    pub operation: i8,
    /// The permission type to match.
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}

