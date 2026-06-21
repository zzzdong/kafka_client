//! Auto-generated from Kafka protocol
//! Message: InitProducerIdResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 22,
    msg_type = "response",
    valid_versions = "0-6",
    flexible_versions = "2+"
)]
pub struct InitProducerIdResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The current producer id.
    #[kafka(versions = "0+", default = -1)]
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    /// The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    #[kafka(versions = "6+", default = -1)]
    pub ongoing_txn_producer_id: i64,
    /// The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    #[kafka(versions = "6+", default = -1)]
    pub ongoing_txn_producer_epoch: i16,
}
