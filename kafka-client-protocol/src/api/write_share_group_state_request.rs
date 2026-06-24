//! Auto-generated from Kafka protocol
//! Message: WriteShareGroupStateRequest
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
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The state epoch of the share-partition.
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    /// The leader epoch of the share-partition.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// The share-partition start offset, or -1 if the start offset is not being written.
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    /// The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
    #[kafka(versions = "1+", default = -1)]
    pub delivery_complete_count: i32,
    /// The state batches for the share-partition.
    #[kafka(versions = "0+")]
    pub state_batches: Vec<StateBatch>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct WriteStateData {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The data for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 85,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct WriteShareGroupStateRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The data for the topics.
    #[kafka(versions = "0+")]
    pub topics: Vec<WriteStateData>,
}
