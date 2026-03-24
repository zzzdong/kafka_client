//! Auto-generated from Kafka protocol
//! Message: OffsetCommitRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetCommitRequestPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The message offset to be committed.
    #[kafka(versions = "0+")]
    pub committed_offset: i64,
    /// The leader epoch of this partition.
    #[kafka(versions = "6+", nullable_versions = "6+", default = -1)]
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub committed_metadata: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetCommitRequestTopic {
    /// The topic name.
    #[kafka(versions = "0-9", nullable_versions = "0-9")]
    pub name: Option<String>,
    /// The topic ID.
    #[kafka(versions = "10+", nullable_versions = "10+")]
    pub topic_id: Option<Uuid>,
    /// Each partition to commit offsets for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetCommitRequestPartition>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 8, msg_type = "request", valid_versions = "2-10", flexible_versions = "8+")]
pub struct OffsetCommitRequest {
    /// The unique group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
    #[kafka(versions = "1+", nullable_versions = "1+", default = -1)]
    pub generation_id_or_member_epoch: i32,
    /// The member ID assigned by the group coordinator.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub member_id: Option<String>,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "7+", nullable_versions = "7+", default = None)]
    pub group_instance_id: Option<String>,
    /// The time period in ms to retain the offset.
    #[kafka(versions = "2-4", nullable_versions = "2-4", default = -1)]
    pub retention_time_ms: i64,
    /// The topics to commit offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetCommitRequestTopic>,
}

