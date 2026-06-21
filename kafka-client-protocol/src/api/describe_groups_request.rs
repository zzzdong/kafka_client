//! Auto-generated from Kafka protocol
//! Message: DescribeGroupsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 15,
    msg_type = "request",
    valid_versions = "0-6",
    flexible_versions = "5+"
)]
pub struct DescribeGroupsRequest {
    /// The names of the groups to describe.
    #[kafka(versions = "0+")]
    pub groups: Vec<String>,
    /// Whether to include authorized operations.
    #[kafka(versions = "3+")]
    pub include_authorized_operations: bool,
}
