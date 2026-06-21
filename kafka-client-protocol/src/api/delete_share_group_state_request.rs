//! Auto-generated from Kafka protocol
//! Message: DeleteShareGroupStateRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteStateData {
    /// The topic identifier.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The data for the partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 86,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct DeleteShareGroupStateRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The data for the topics.
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteStateData>,
}
