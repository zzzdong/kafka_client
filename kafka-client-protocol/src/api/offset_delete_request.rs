//! Auto-generated from Kafka protocol
//! Message: OffsetDeleteRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetDeleteRequestPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetDeleteRequestTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// Each partition to delete offsets for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetDeleteRequestPartition>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 47, msg_type = "request", valid_versions = "0", flexible_versions = "none")]
pub struct OffsetDeleteRequest {
    /// The unique group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The topics to delete offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetDeleteRequestTopic>,
}

