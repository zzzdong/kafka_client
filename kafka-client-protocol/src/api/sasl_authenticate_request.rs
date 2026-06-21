//! Auto-generated from Kafka protocol
//! Message: SaslAuthenticateRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 36,
    msg_type = "request",
    valid_versions = "0-2",
    flexible_versions = "2+"
)]
pub struct SaslAuthenticateRequest {
    /// The SASL authentication bytes from the client, as defined by the SASL mechanism.
    #[kafka(versions = "0+")]
    pub auth_bytes: Bytes,
}
