//! Auto-generated from Kafka protocol
//! Message: EndTxnRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 26, msg_type = "request", valid_versions = "0-5", flexible_versions = "3+")]
pub struct EndTxnRequest {
    /// The ID of the transaction to end.
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    /// The producer ID.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The current epoch associated with the producer.
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    /// True if the transaction was committed, false if it was aborted.
    #[kafka(versions = "0+")]
    pub committed: bool,
}

