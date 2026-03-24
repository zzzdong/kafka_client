//! Auto-generated from Kafka protocol
//! Message: SaslHandshakeRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 17, msg_type = "request", valid_versions = "0-1", flexible_versions = "none")]
pub struct SaslHandshakeRequest {
    /// The SASL mechanism chosen by the client.
    #[kafka(versions = "0+")]
    pub mechanism: String,
}

