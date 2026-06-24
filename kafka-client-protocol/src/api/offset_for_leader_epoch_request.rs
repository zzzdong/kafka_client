//! Auto-generated from Kafka protocol
//! Message: OffsetForLeaderEpochRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetForLeaderPartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
    #[kafka(versions = "2+", default = -1)]
    pub current_leader_epoch: i32,
    /// The epoch to look up an offset for.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetForLeaderTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic: String,
    /// Each partition to get offsets for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetForLeaderPartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 23,
    msg_type = "request",
    valid_versions = "2-4",
    flexible_versions = "4+"
)]
pub struct OffsetForLeaderEpochRequest {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    #[kafka(versions = "3+", default = -2)]
    pub replica_id: i32,
    /// Each topic to get offsets for.
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetForLeaderTopic>,
}
