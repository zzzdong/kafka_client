//! Auto-generated from Kafka protocol
//! Message: DeleteGroupsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeletableGroupResult {
    /// The group id.
    #[kafka(versions = "0+", map_key)]
    pub group_id: String,
    /// The deletion error, or 0 if the deletion succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 42, msg_type = "response", valid_versions = "0-2", flexible_versions = "2+")]
pub struct DeleteGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The deletion results.
    #[kafka(versions = "0+")]
    pub results: Vec<DeletableGroupResult>,
}

