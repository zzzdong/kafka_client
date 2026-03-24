//! Auto-generated from Kafka protocol
//! Message: CreateAclsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AclCreationResult {
    /// The result error, or zero if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The result message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 30, msg_type = "response", valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateAclsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The results for each ACL creation.
    #[kafka(versions = "0+")]
    pub results: Vec<AclCreationResult>,
}

