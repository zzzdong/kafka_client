//! Auto-generated from Kafka protocol
//! Message: ExpireDelegationTokenResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 40,
    msg_type = "response",
    valid_versions = "1-2",
    flexible_versions = "2+"
)]
pub struct ExpireDelegationTokenResponse {
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The timestamp in milliseconds at which this token expires.
    #[kafka(versions = "0+")]
    pub expiry_timestamp_ms: i64,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
}
