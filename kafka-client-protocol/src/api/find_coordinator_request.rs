//! Auto-generated from Kafka protocol
//! Message: FindCoordinatorRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 10,
    msg_type = "request",
    valid_versions = "0-6",
    flexible_versions = "3+"
)]
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    #[kafka(versions = "0-3")]
    pub key: String,
    /// The coordinator key type. (group, transaction, share).
    #[kafka(versions = "1+", default = 0)]
    pub key_type: i8,
    /// The coordinator keys.
    #[kafka(versions = "4+")]
    pub coordinator_keys: Vec<String>,
}
