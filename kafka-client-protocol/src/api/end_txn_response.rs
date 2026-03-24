//! Auto-generated from Kafka protocol
//! Message: EndTxnResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 26, msg_type = "response", valid_versions = "0-5", flexible_versions = "3+")]
pub struct EndTxnResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The producer ID.
    #[kafka(versions = "5+", nullable_versions = "5+", default = -1)]
    pub producer_id: i64,
    /// The current epoch associated with the producer.
    #[kafka(versions = "5+", nullable_versions = "5+", default = -1)]
    pub producer_epoch: i16,
}

