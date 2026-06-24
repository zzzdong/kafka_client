//! Auto-generated from Kafka protocol
//! Message: BrokerHeartbeatResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 63,
    msg_type = "response",
    valid_versions = "0-2",
    flexible_versions = "0+"
)]
pub struct BrokerHeartbeatResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// True if the broker has approximately caught up with the latest metadata.
    #[kafka(versions = "0+", default = false)]
    pub is_caught_up: bool,
    /// True if the broker is fenced.
    #[kafka(versions = "0+", default = true)]
    pub is_fenced: bool,
    /// True if the broker should proceed with its shutdown.
    #[kafka(versions = "0+")]
    pub should_shut_down: bool,
}
