//! Auto-generated from Kafka protocol
//! Message: ListPartitionReassignmentsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ListPartitionReassignmentsTopics {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The partitions to list partition reassignments for.
    #[kafka(versions = "0+")]
    pub partition_indexes: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 46,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct ListPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    #[kafka(versions = "0+", default = 60000)]
    pub timeout_ms: i32,
    /// The topics to list partition reassignments for, or null to list everything.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub topics: Option<Vec<ListPartitionReassignmentsTopics>>,
}
