//! Auto-generated from Kafka protocol
//! Message: ShareGroupDescribeRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 77,
    msg_type = "request",
    valid_versions = "1",
    flexible_versions = "0+"
)]
pub struct ShareGroupDescribeRequest {
    /// The ids of the groups to describe.
    #[kafka(versions = "0+")]
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    #[kafka(versions = "0+")]
    pub include_authorized_operations: bool,
}
