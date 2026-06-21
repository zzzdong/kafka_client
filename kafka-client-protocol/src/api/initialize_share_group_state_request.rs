//! Auto-generated from Kafka protocol
//! Message: InitializeShareGroupStateRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The state epoch for this share-partition.
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    /// The share-partition start offset, or -1 if the start offset is not being initialized.
    #[kafka(versions = "0+")]
    pub start_offset: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct InitializeStateData {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The data for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 83,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct InitializeShareGroupStateRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The data for the topics.
    #[kafka(versions = "0+")]
    pub topics: Vec<InitializeStateData>,
}
