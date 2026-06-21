//! Auto-generated from Kafka protocol
//! Message: ListTransactionsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TransactionState {
    /// The transactional id.
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    /// The producer id.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The current transaction state of the producer.
    #[kafka(versions = "0+")]
    pub transaction_state: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 66,
    msg_type = "response",
    valid_versions = "0-2",
    flexible_versions = "0+"
)]
pub struct ListTransactionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// Set of state filters provided in the request which were unknown to the transaction coordinator.
    #[kafka(versions = "0+")]
    pub unknown_state_filters: Vec<String>,
    /// The current state of the transaction for the transactional id.
    #[kafka(versions = "0+")]
    pub transaction_states: Vec<TransactionState>,
}
