//! Auto-generated from Kafka protocol
//! Message: StreamsGroupDescribeRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 89, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct StreamsGroupDescribeRequest {
    /// The ids of the groups to describe
    #[kafka(versions = "0+")]
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    #[kafka(versions = "0+")]
    pub include_authorized_operations: bool,
}

