//! Auto-generated from Kafka protocol
//! Message: FindCoordinatorResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Coordinator {
    /// The coordinator key.
    #[kafka(versions = "4+")]
    pub key: String,
    /// The node id.
    #[kafka(versions = "4+")]
    pub node_id: i32,
    /// The host name.
    #[kafka(versions = "4+")]
    pub host: String,
    /// The port.
    #[kafka(versions = "4+")]
    pub port: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "4+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "4+", nullable_versions = "4+")]
    pub error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 10, msg_type = "response", valid_versions = "0-6", flexible_versions = "3+")]
pub struct FindCoordinatorResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0-3")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "1-3", nullable_versions = "1-3")]
    pub error_message: Option<String>,
    /// The node id.
    #[kafka(versions = "0-3")]
    pub node_id: i32,
    /// The host name.
    #[kafka(versions = "0-3")]
    pub host: String,
    /// The port.
    #[kafka(versions = "0-3")]
    pub port: i32,
    /// Each coordinator result in the response.
    #[kafka(versions = "4+")]
    pub coordinators: Vec<Coordinator>,
}

