//! Auto-generated from Kafka protocol
//! Message: RenewDelegationTokenResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 39, msg_type = "response", valid_versions = "1-2", flexible_versions = "2+")]
pub struct RenewDelegationTokenResponse {
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

