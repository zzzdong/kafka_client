//! Auto-generated from Kafka protocol
//! Message: AddPartitionsToTxnResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AddPartitionsToTxnTopicResult {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The results for each partition.
    #[kafka(versions = "0+")]
    pub results_by_partition: Vec<AddPartitionsToTxnPartitionResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AddPartitionsToTxnPartitionResult {
    /// The partition indexes.
    #[kafka(versions = "0+", map_key)]
    pub partition_index: i32,
    /// The response error code.
    #[kafka(versions = "0+")]
    pub partition_error_code: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AddPartitionsToTxnResult {
    /// The transactional id corresponding to the transaction.
    #[kafka(versions = "4+", map_key)]
    pub transactional_id: String,
    /// The results for each topic.
    #[kafka(versions = "4+")]
    pub topic_results: Vec<AddPartitionsToTxnTopicResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 24,
    msg_type = "response",
    valid_versions = "0-5",
    flexible_versions = "3+"
)]
pub struct AddPartitionsToTxnResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The response top level error code.
    #[kafka(versions = "4+", nullable_versions = "4+")]
    pub error_code: i16,
    /// Results categorized by transactional ID.
    #[kafka(versions = "4+")]
    pub results_by_transaction: Vec<AddPartitionsToTxnResult>,
    /// The results for each topic.
    #[kafka(versions = "0-3")]
    pub results_by_topic_v3_and_below: Vec<AddPartitionsToTxnTopicResult>,
}
