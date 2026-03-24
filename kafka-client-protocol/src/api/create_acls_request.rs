//! Auto-generated from Kafka protocol
//! Message: CreateAclsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AclCreation {
    /// The type of the resource.
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    /// The resource name for the ACL.
    #[kafka(versions = "0+")]
    pub resource_name: String,
    /// The pattern type for the ACL.
    #[kafka(versions = "1+", default = 3)]
    pub resource_pattern_type: i8,
    /// The principal for the ACL.
    #[kafka(versions = "0+")]
    pub principal: String,
    /// The host for the ACL.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The operation type for the ACL (read, write, etc.).
    #[kafka(versions = "0+")]
    pub operation: i8,
    /// The permission type for the ACL (allow, deny, etc.).
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 30, msg_type = "request", valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateAclsRequest {
    /// The ACLs that we want to create.
    #[kafka(versions = "0+")]
    pub creations: Vec<AclCreation>,
}

