//! Auto-generated from Kafka protocol
//! Message: ReadShareGroupStateSummaryRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The leader epoch of the share-partition.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReadStateSummaryData {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The data for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 87,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct ReadShareGroupStateSummaryRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The data for the topics.
    #[kafka(versions = "0+")]
    pub topics: Vec<ReadStateSummaryData>,
}
