//! Auto-generated from Kafka protocol
//! Message: EndTxnMarker
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "none")]
pub struct EndTxnMarker {
    /// The coordinator epoch when appending the record
    #[kafka(versions = "0+")]
    pub coordinator_epoch: i32,
}

