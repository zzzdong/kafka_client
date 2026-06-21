//! Auto-generated from Kafka protocol
//! Message: AlterShareGroupOffsetsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterShareGroupOffsetsRequestPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The share-partition start offset.
    #[kafka(versions = "0+")]
    pub start_offset: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterShareGroupOffsetsRequestTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic_name: String,
    /// Each partition to alter offsets for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterShareGroupOffsetsRequestPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 91,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct AlterShareGroupOffsetsRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The topics to alter offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterShareGroupOffsetsRequestTopic>,
}
