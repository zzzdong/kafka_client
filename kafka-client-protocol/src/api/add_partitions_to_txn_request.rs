//! Auto-generated from Kafka protocol
//! Message: AddPartitionsToTxnRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AddPartitionsToTxnTopic {
    /// The name of the topic.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The partition indexes to add to the transaction.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AddPartitionsToTxnTransaction {
    /// The transactional id corresponding to the transaction.
    #[kafka(versions = "4+", map_key)]
    pub transactional_id: String,
    /// Current producer id in use by the transactional id.
    #[kafka(versions = "4+")]
    pub producer_id: i64,
    /// Current epoch associated with the producer id.
    #[kafka(versions = "4+")]
    pub producer_epoch: i16,
    /// Boolean to signify if we want to check if the partition is in the transaction rather than add it.
    #[kafka(versions = "4+", default = false)]
    pub verify_only: bool,
    /// The partitions to add to the transaction.
    #[kafka(versions = "4+")]
    pub topics: Vec<AddPartitionsToTxnTopic>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 24, msg_type = "request", valid_versions = "0-5", flexible_versions = "3+")]
pub struct AddPartitionsToTxnRequest {
    /// List of transactions to add partitions to.
    #[kafka(versions = "4+")]
    pub transactions: Vec<AddPartitionsToTxnTransaction>,
    /// The transactional id corresponding to the transaction.
    #[kafka(versions = "0-3")]
    pub v3_and_below_transactional_id: String,
    /// Current producer id in use by the transactional id.
    #[kafka(versions = "0-3")]
    pub v3_and_below_producer_id: i64,
    /// Current epoch associated with the producer id.
    #[kafka(versions = "0-3")]
    pub v3_and_below_producer_epoch: i16,
    /// The partitions to add to the transaction.
    #[kafka(versions = "0-3")]
    pub v3_and_below_topics: Vec<AddPartitionsToTxnTopic>,
}

