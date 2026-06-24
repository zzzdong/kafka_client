//! Auto-generated from Kafka protocol
//! Message: ListGroupsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ListedGroup {
    /// The group ID.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The group protocol type.
    #[kafka(versions = "0+")]
    pub protocol_type: String,
    /// The group state name.
    #[kafka(versions = "4+")]
    pub group_state: String,
    /// The group type name.
    #[kafka(versions = "5+")]
    pub group_type: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 16,
    msg_type = "response",
    valid_versions = "0-5",
    flexible_versions = "3+"
)]
pub struct ListGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// Each group in the response.
    #[kafka(versions = "0+")]
    pub groups: Vec<ListedGroup>,
}
