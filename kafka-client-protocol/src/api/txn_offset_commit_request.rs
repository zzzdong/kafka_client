//! Auto-generated from Kafka protocol
//! Message: TxnOffsetCommitRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TxnOffsetCommitRequestPartition {
    /// The index of the partition within the topic.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The message offset to be committed.
    #[kafka(versions = "0+")]
    pub committed_offset: i64,
    /// The leader epoch of the last consumed record.
    #[kafka(versions = "2+", default = -1)]
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub committed_metadata: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TxnOffsetCommitRequestTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The partitions inside the topic that we want to commit offsets for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<TxnOffsetCommitRequestPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 28,
    msg_type = "request",
    valid_versions = "0-5",
    flexible_versions = "3+"
)]
pub struct TxnOffsetCommitRequest {
    /// The ID of the transaction.
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    /// The ID of the group.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The current producer ID in use by the transactional ID.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    /// The generation of the consumer.
    #[kafka(versions = "3+", default = -1)]
    pub generation_id: i32,
    /// The member ID assigned by the group coordinator.
    #[kafka(versions = "3+", default = "")]
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "3+", nullable_versions = "3+", default = None)]
    pub group_instance_id: Option<String>,
    /// Each topic that we want to commit offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}
