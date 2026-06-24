//! Auto-generated from Kafka protocol
//! Message: ControlRecordTypeSchema
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "none")]
pub struct ControlRecordTypeSchema {
    /// The type of the control record, such as commit or abort
    #[kafka(versions = "0+")]
    pub r#type: i16,
}
