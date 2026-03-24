//! Auto-generated from Kafka protocol
//! Message: CreatePartitionsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatePartitionsTopicResult {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The result error, or zero if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The result message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 37, msg_type = "response", valid_versions = "0-3", flexible_versions = "2+")]
pub struct CreatePartitionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The partition creation results for each topic.
    #[kafka(versions = "0+")]
    pub results: Vec<CreatePartitionsTopicResult>,
}

