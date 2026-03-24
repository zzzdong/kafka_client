//! Auto-generated from Kafka protocol
//! Message: SnapshotFooterRecord
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "0+")]
pub struct SnapshotFooterRecord {
    /// The version of the snapshot footer record.
    #[kafka(versions = "0+")]
    pub version: i16,
}

