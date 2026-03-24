//! Auto-generated from Kafka protocol
//! Message: EnvelopeRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 58, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct EnvelopeRequest {
    /// The embedded request header and data.
    #[kafka(versions = "0+")]
    pub request_data: Bytes,
    /// Value of the initial client principal when the request is redirected by a broker.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub request_principal: Option<Bytes>,
    /// The original client's address in bytes.
    #[kafka(versions = "0+")]
    pub client_host_address: Bytes,
}

