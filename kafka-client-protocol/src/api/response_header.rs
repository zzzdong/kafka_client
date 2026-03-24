//! Auto-generated from Kafka protocol
//! Message: ResponseHeader
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "header", valid_versions = "0-1", flexible_versions = "1+")]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    #[kafka(versions = "0+")]
    pub correlation_id: i32,
}

