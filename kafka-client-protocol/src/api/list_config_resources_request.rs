//! Auto-generated from Kafka protocol
//! Message: ListConfigResourcesRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 74,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct ListConfigResourcesRequest {
    /// The list of resource type. If the list is empty, it uses default supported config resource types.
    #[kafka(versions = "1+")]
    pub resource_types: Vec<i8>,
}
