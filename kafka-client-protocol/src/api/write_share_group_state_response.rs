//! Auto-generated from Kafka protocol
//! Message: WriteShareGroupStateResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
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
pub struct WriteStateResult {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The results for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionResult>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 85, msg_type = "response", valid_versions = "0-1", flexible_versions = "0+")]
pub struct WriteShareGroupStateResponse {
    /// The write results.
    #[kafka(versions = "0+")]
    pub results: Vec<WriteStateResult>,
}

