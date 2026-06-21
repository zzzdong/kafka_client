//! Auto-generated from Kafka protocol
//! Message: AddOffsetsToTxnRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 25,
    msg_type = "request",
    valid_versions = "0-4",
    flexible_versions = "3+"
)]
pub struct AddOffsetsToTxnRequest {
    /// The transactional id corresponding to the transaction.
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    /// Current producer id in use by the transactional id.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// Current epoch associated with the producer id.
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    /// The unique group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
}
