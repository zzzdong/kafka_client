//! Auto-generated from Kafka protocol
//! Message: DeleteTopicsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteTopicState {
    /// The topic name.
    #[kafka(versions = "6+", nullable_versions = "6+", default = None)]
    pub name: Option<String>,
    /// The unique topic ID.
    #[kafka(versions = "6+")]
    pub topic_id: Uuid,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 20,
    msg_type = "request",
    valid_versions = "1-6",
    flexible_versions = "4+"
)]
pub struct DeleteTopicsRequest {
    /// The name or topic ID of the topic.
    #[kafka(versions = "6+")]
    pub topics: Vec<DeleteTopicState>,
    /// The names of the topics to delete.
    #[kafka(versions = "0-5")]
    pub topic_names: Vec<String>,
    /// The length of time in milliseconds to wait for the deletions to complete.
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
}
