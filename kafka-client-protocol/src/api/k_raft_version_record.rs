//! Auto-generated from Kafka protocol
//! Message: KRaftVersionRecord
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "0+")]
pub struct KraftVersionRecord {
    /// The version of the kraft version record.
    #[kafka(versions = "0+")]
    pub version: i16,
    /// The kraft protocol version.
    #[kafka(versions = "0+")]
    pub kraft_version: i16,
}

