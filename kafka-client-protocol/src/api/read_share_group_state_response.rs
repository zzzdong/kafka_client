//! Auto-generated from Kafka protocol
//! Message: ReadShareGroupStateResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct StateBatch {
    /// The first offset of this state batch.
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    /// The last offset of this state batch.
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    /// The delivery state - 0:Available,2:Acked,4:Archived.
    #[kafka(versions = "0+")]
    pub delivery_state: i8,
    /// The delivery count.
    #[kafka(versions = "0+")]
    pub delivery_count: i16,
}

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
    /// The share-partition start offset, which can be -1 if it is not yet initialized.
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    /// The state batches for this share-partition.
    #[kafka(versions = "0+")]
    pub state_batches: Vec<StateBatch>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReadStateResult {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The results for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 84,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct ReadShareGroupStateResponse {
    /// The read results.
    #[kafka(versions = "0+")]
    pub results: Vec<ReadStateResult>,
}
