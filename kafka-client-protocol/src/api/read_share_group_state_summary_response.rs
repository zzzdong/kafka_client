//! Auto-generated from Kafka protocol
//! Message: ReadShareGroupStateSummaryResponse
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
    /// The state epoch of the share-partition.
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    /// The leader epoch of the share-partition.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// The share-partition start offset.
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    /// The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
    #[kafka(versions = "1+", default = -1)]
    pub delivery_complete_count: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReadStateSummaryResult {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The results for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 87,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct ReadShareGroupStateSummaryResponse {
    /// The read results.
    #[kafka(versions = "0+")]
    pub results: Vec<ReadStateSummaryResult>,
}
