//! Auto-generated from Kafka protocol
//! Message: DescribeTransactionsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic: String,
    /// The partition ids included in the current transaction.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TransactionState {
    /// The error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The transactional id.
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    /// The current transaction state of the producer.
    #[kafka(versions = "0+")]
    pub transaction_state: String,
    /// The timeout in milliseconds for the transaction.
    #[kafka(versions = "0+")]
    pub transaction_timeout_ms: i32,
    /// The start time of the transaction in milliseconds.
    #[kafka(versions = "0+")]
    pub transaction_start_time_ms: i64,
    /// The current producer id associated with the transaction.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    /// The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 65, msg_type = "response", valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeTransactionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The current state of the transaction.
    #[kafka(versions = "0+")]
    pub transaction_states: Vec<TransactionState>,
}

