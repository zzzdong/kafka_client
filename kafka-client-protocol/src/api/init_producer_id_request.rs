//! Auto-generated from Kafka protocol
//! Message: InitProducerIdRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 22,
    msg_type = "request",
    valid_versions = "0-6",
    flexible_versions = "2+"
)]
pub struct InitProducerIdRequest {
    /// The transactional id, or null if the producer is not transactional.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub transactional_id: Option<String>,
    /// The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    #[kafka(versions = "0+")]
    pub transaction_timeout_ms: i32,
    /// The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
    #[kafka(versions = "3+", default = -1)]
    pub producer_id: i64,
    /// The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
    #[kafka(versions = "3+", default = -1)]
    pub producer_epoch: i16,
    /// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
    #[kafka(versions = "6+", default = false)]
    pub enable2_pc: bool,
    /// True if the client wants to keep the currently ongoing transaction instead of aborting it.
    #[kafka(versions = "6+", default = false)]
    pub keep_prepared_txn: bool,
}
