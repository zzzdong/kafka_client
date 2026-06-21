//! Auto-generated from Kafka protocol
//! Message: ListGroupsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 16,
    msg_type = "request",
    valid_versions = "0-5",
    flexible_versions = "3+"
)]
pub struct ListGroupsRequest {
    /// The states of the groups we want to list. If empty, all groups are returned with their state.
    #[kafka(versions = "4+")]
    pub states_filter: Vec<String>,
    /// The types of the groups we want to list. If empty, all groups are returned with their type.
    #[kafka(versions = "5+")]
    pub types_filter: Vec<String>,
}
