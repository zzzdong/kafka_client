//! Auto-generated from Kafka protocol
//! Message: GetTelemetrySubscriptionsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 71,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct GetTelemetrySubscriptionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
    #[kafka(versions = "0+")]
    pub client_instance_id: Uuid,
    /// Unique identifier for the current subscription set for this client instance.
    #[kafka(versions = "0+")]
    pub subscription_id: i32,
    /// Compression types that broker accepts for the PushTelemetryRequest.
    #[kafka(versions = "0+")]
    pub accepted_compression_types: Vec<i8>,
    /// Configured push interval, which is the lowest configured interval in the current subscription set.
    #[kafka(versions = "0+")]
    pub push_interval_ms: i32,
    /// The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
    #[kafka(versions = "0+")]
    pub telemetry_max_bytes: i32,
    /// Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
    #[kafka(versions = "0+")]
    pub delta_temporality: bool,
    /// Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
    #[kafka(versions = "0+")]
    pub requested_metrics: Vec<String>,
}
