//! Auto-generated from Kafka protocol
//! Message: UnregisterBrokerRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 64,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct UnregisterBrokerRequest {
    /// The broker ID to unregister.
    #[kafka(versions = "0+")]
    pub broker_id: i32,
}
