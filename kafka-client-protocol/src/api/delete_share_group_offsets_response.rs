//! Auto-generated from Kafka protocol
//! Message: DeleteShareGroupOffsetsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteShareGroupOffsetsResponseTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The unique topic ID.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The topic-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The topic-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 92, msg_type = "response", valid_versions = "0", flexible_versions = "0+")]
pub struct DeleteShareGroupOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The results for each topic.
    #[kafka(versions = "0+")]
    pub responses: Vec<DeleteShareGroupOffsetsResponseTopic>,
}

