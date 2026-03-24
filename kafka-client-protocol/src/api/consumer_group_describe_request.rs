//! Auto-generated from Kafka protocol
//! Message: ConsumerGroupDescribeRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 69, msg_type = "request", valid_versions = "0-1", flexible_versions = "0+")]
pub struct ConsumerGroupDescribeRequest {
    /// The ids of the groups to describe.
    #[kafka(versions = "0+")]
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    #[kafka(versions = "0+")]
    pub include_authorized_operations: bool,
}

