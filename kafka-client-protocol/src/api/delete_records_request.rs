//! Auto-generated from Kafka protocol
//! Message: DeleteRecordsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteRecordsPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The deletion offset. -1 means that records should be truncated to the high watermark.
    #[kafka(versions = "0+")]
    pub offset: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteRecordsTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// Each partition that we want to delete records from.
    #[kafka(versions = "0+")]
    pub partitions: Vec<DeleteRecordsPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 21,
    msg_type = "request",
    valid_versions = "0-2",
    flexible_versions = "2+"
)]
pub struct DeleteRecordsRequest {
    /// Each topic that we want to delete records from.
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteRecordsTopic>,
    /// How long to wait for the deletion to complete, in milliseconds.
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
}
