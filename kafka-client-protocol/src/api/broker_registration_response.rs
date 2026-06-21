//! Auto-generated from Kafka protocol
//! Message: BrokerRegistrationResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 62,
    msg_type = "response",
    valid_versions = "0-4",
    flexible_versions = "0+"
)]
pub struct BrokerRegistrationResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The broker's assigned epoch, or -1 if none was assigned.
    #[kafka(versions = "0+", default = -1)]
    pub broker_epoch: i64,
}
