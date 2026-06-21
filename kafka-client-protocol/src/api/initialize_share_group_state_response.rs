//! Auto-generated from Kafka protocol
//! Message: InitializeShareGroupStateResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionResult {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct InitializeStateResult {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The results for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 83,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct InitializeShareGroupStateResponse {
    /// The initialization results.
    #[kafka(versions = "0+")]
    pub results: Vec<InitializeStateResult>,
}
