//! Auto-generated from Kafka protocol
//! Message: DescribeAclsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AclDescription {
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
pub struct DescribeAclsResource {
    /// The resource type.
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    /// The resource name.
    #[kafka(versions = "0+")]
    pub resource_name: String,
    /// The resource pattern type.
    #[kafka(versions = "1+", default = 3)]
    pub pattern_type: i8,
    /// The ACLs.
    #[kafka(versions = "0+")]
    pub acls: Vec<AclDescription>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 29, msg_type = "response", valid_versions = "1-3", flexible_versions = "2+")]
pub struct DescribeAclsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// Each Resource that is referenced in an ACL.
    #[kafka(versions = "0+")]
    pub resources: Vec<DescribeAclsResource>,
}

