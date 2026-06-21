//! Auto-generated from Kafka protocol
//! Message: SyncGroupResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 14,
    msg_type = "response",
    valid_versions = "0-5",
    flexible_versions = "4+"
)]
pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The group protocol type.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub protocol_type: Option<String>,
    /// The group protocol name.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub protocol_name: Option<String>,
    /// The member assignment.
    #[kafka(versions = "0+")]
    pub assignment: Bytes,
}
