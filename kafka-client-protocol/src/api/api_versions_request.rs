//! Auto-generated from Kafka protocol
//! Message: ApiVersionsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 18,
    msg_type = "request",
    valid_versions = "0-4",
    flexible_versions = "3+"
)]
pub struct ApiVersionsRequest {
    /// The name of the client.
    #[kafka(versions = "3+")]
    pub client_software_name: String,
    /// The version of the client.
    #[kafka(versions = "3+")]
    pub client_software_version: String,
}
