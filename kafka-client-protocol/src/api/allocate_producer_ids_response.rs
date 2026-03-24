//! Auto-generated from Kafka protocol
//! Message: AllocateProducerIdsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 67, msg_type = "response", valid_versions = "0", flexible_versions = "0+")]
pub struct AllocateProducerIdsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The first producer ID in this range, inclusive.
    #[kafka(versions = "0+")]
    pub producer_id_start: i64,
    /// The number of producer IDs in this range.
    #[kafka(versions = "0+")]
    pub producer_id_len: i32,
}

