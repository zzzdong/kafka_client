//! Auto-generated from Kafka protocol
//! Message: DeleteAclsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteAclsMatchingAcl {
    /// The deletion error code, or 0 if the deletion succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The deletion error message, or null if the deletion succeeded.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The ACL resource type.
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    /// The ACL resource name.
    #[kafka(versions = "0+")]
    pub resource_name: String,
    /// The ACL resource pattern type.
    #[kafka(versions = "1+", default = 3)]
    pub pattern_type: i8,
    /// The ACL principal.
    #[kafka(versions = "0+")]
    pub principal: String,
    /// The ACL host.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The ACL operation.
    #[kafka(versions = "0+")]
    pub operation: i8,
    /// The ACL permission type.
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteAclsFilterResult {
    /// The error code, or 0 if the filter succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if the filter succeeded.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The ACLs which matched this filter.
    #[kafka(versions = "0+")]
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 31, msg_type = "response", valid_versions = "1-3", flexible_versions = "2+")]
pub struct DeleteAclsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The results for each filter.
    #[kafka(versions = "0+")]
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

