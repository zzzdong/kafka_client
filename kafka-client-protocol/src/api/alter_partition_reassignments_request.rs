//! Auto-generated from Kafka protocol
//! Message: AlterPartitionReassignmentsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReassignablePartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub replicas: Option<Vec<i32>>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReassignableTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The partitions to reassign.
    #[kafka(versions = "0+")]
    pub partitions: Vec<ReassignablePartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 45,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct AlterPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    #[kafka(versions = "0+", default = 60000)]
    pub timeout_ms: i32,
    /// The option indicating whether changing the replication factor of any given partition as part of this request is a valid move.
    #[kafka(versions = "1+", default = true)]
    pub allow_replication_factor_change: bool,
    /// The topics to reassign.
    #[kafka(versions = "0+")]
    pub topics: Vec<ReassignableTopic>,
}
