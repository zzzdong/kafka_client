//! Auto-generated from Kafka protocol
//! Message: ListPartitionReassignmentsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OngoingPartitionReassignment {
    /// The index of the partition.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The current replica set.
    #[kafka(versions = "0+")]
    pub replicas: Vec<i32>,
    /// The set of replicas we are currently adding.
    #[kafka(versions = "0+")]
    pub adding_replicas: Vec<i32>,
    /// The set of replicas we are currently removing.
    #[kafka(versions = "0+")]
    pub removing_replicas: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OngoingTopicReassignment {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The ongoing reassignments for each partition.
    #[kafka(versions = "0+")]
    pub partitions: Vec<OngoingPartitionReassignment>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 46,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct ListPartitionReassignmentsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
    /// The ongoing reassignments for each topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<OngoingTopicReassignment>,
}
