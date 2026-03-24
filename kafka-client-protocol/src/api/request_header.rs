//! Auto-generated from Kafka protocol
//! Message: RequestHeader
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "header", valid_versions = "1-2", flexible_versions = "2+")]
pub struct RequestHeader {
    /// The API key of this request.
    #[kafka(versions = "0+")]
    pub request_api_key: i16,
    /// The API version of this request.
    #[kafka(versions = "0+")]
    pub request_api_version: i16,
    /// The correlation ID of this request.
    #[kafka(versions = "0+")]
    pub correlation_id: i32,
    /// The client ID string.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub client_id: Option<String>,
}

