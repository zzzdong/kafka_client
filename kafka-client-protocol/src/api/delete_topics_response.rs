//! Auto-generated from Kafka protocol
//! Message: DeleteTopicsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeletableTopicResult {
    /// The topic name.
    #[kafka(versions = "0+", nullable_versions = "6+", map_key)]
    pub name: Option<String>,
    /// The unique topic ID.
    #[kafka(versions = "6+", nullable_versions = "6+")]
    pub topic_id: Option<Uuid>,
    /// The deletion error, or 0 if the deletion succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 20,
    msg_type = "response",
    valid_versions = "1-6",
    flexible_versions = "4+"
)]
pub struct DeleteTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// The results for each topic we tried to delete.
    #[kafka(versions = "0+")]
    pub responses: Vec<DeletableTopicResult>,
}
