//! Auto-generated from Kafka protocol
//! Message: CreatePartitionsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatePartitionsAssignment {
    /// The assigned broker IDs.
    #[kafka(versions = "0+")]
    pub broker_ids: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatePartitionsTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The new partition count.
    #[kafka(versions = "0+")]
    pub count: i32,
    /// The new partition assignments.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 37,
    msg_type = "request",
    valid_versions = "0-3",
    flexible_versions = "2+"
)]
pub struct CreatePartitionsRequest {
    /// Each topic that we want to create new partitions inside.
    #[kafka(versions = "0+")]
    pub topics: Vec<CreatePartitionsTopic>,
    /// The time in ms to wait for the partitions to be created.
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    /// If true, then validate the request, but don't actually increase the number of partitions.
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}
