//! Auto-generated from Kafka protocol
//! Message: GetTelemetrySubscriptionsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 71,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct GetTelemetrySubscriptionsRequest {
    /// Unique id for this client instance, must be set to 0 on the first request.
    #[kafka(versions = "0+")]
    pub client_instance_id: Uuid,
}
