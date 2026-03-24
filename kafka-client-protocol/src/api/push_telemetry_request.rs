//! Auto-generated from Kafka protocol
//! Message: PushTelemetryRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 72, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct PushTelemetryRequest {
    /// Unique id for this client instance.
    #[kafka(versions = "0+")]
    pub client_instance_id: Uuid,
    /// Unique identifier for the current subscription.
    #[kafka(versions = "0+")]
    pub subscription_id: i32,
    /// Client is terminating the connection.
    #[kafka(versions = "0+")]
    pub terminating: bool,
    /// Compression codec used to compress the metrics.
    #[kafka(versions = "0+")]
    pub compression_type: i8,
    /// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
    #[kafka(versions = "0+")]
    pub metrics: Bytes,
}

