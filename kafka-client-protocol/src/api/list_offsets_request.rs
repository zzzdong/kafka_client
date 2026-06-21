//! Auto-generated from Kafka protocol
//! Message: ListOffsetsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ListOffsetsPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The current leader epoch.
    #[kafka(versions = "4+", nullable_versions = "4+", default = -1)]
    pub current_leader_epoch: i32,
    /// The current timestamp.
    #[kafka(versions = "0+")]
    pub timestamp: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ListOffsetsTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// Each partition in the request.
    #[kafka(versions = "0+")]
    pub partitions: Vec<ListOffsetsPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 2,
    msg_type = "request",
    valid_versions = "1-11",
    flexible_versions = "6+"
)]
pub struct ListOffsetsRequest {
    /// The broker ID of the requester, or -1 if this request is being made by a normal consumer.
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
    #[kafka(versions = "2+")]
    pub isolation_level: i8,
    /// Each topic in the request.
    #[kafka(versions = "0+")]
    pub topics: Vec<ListOffsetsTopic>,
    /// The timeout to await a response in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage.
    #[kafka(versions = "10+", nullable_versions = "10+")]
    pub timeout_ms: i32,
}
