//! Auto-generated from Kafka protocol
//! Message: DeleteAclsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteAclsFilter {
    /// The resource type.
    #[kafka(versions = "0+")]
    pub resource_type_filter: i8,
    /// The resource name, or null to match any resource name.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub resource_name_filter: Option<String>,
    /// The pattern type.
    #[kafka(versions = "1+", default = 3)]
    pub pattern_type_filter: i8,
    /// The principal filter, or null to accept all principals.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub principal_filter: Option<String>,
    /// The host filter, or null to accept all hosts.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub host_filter: Option<String>,
    /// The ACL operation.
    #[kafka(versions = "0+")]
    pub operation: i8,
    /// The permission type.
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 31, msg_type = "request", valid_versions = "1-3", flexible_versions = "2+")]
pub struct DeleteAclsRequest {
    /// The filters to use when deleting ACLs.
    #[kafka(versions = "0+")]
    pub filters: Vec<DeleteAclsFilter>,
}

