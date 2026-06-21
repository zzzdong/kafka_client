//! Auto-generated from Kafka protocol
//! Message: DeleteGroupsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 42,
    msg_type = "request",
    valid_versions = "0-2",
    flexible_versions = "2+"
)]
pub struct DeleteGroupsRequest {
    /// The group names to delete.
    #[kafka(versions = "0+")]
    pub groups_names: Vec<String>,
}
