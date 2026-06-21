//! Auto-generated from Kafka protocol
//! Message: SnapshotHeaderRecord
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "0+")]
pub struct SnapshotHeaderRecord {
    /// The version of the snapshot header record.
    #[kafka(versions = "0+")]
    pub version: i16,
    /// The append time of the last record from the log contained in this snapshot.
    #[kafka(versions = "0+")]
    pub last_contained_log_timestamp: i64,
}
