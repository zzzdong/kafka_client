//! Auto-generated from Kafka protocol
//! Message: FetchSnapshotResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct SnapshotId {
    /// The snapshot end offset.
    #[kafka(versions = "0+")]
    pub end_offset: i64,
    /// The snapshot epoch.
    #[kafka(versions = "0+")]
    pub epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionSnapshot {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub index: i32,
    /// The error code, or 0 if there was no fetch error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The snapshot endOffset and epoch fetched.
    #[kafka(versions = "0+")]
    pub snapshot_id: SnapshotId,
    /// The leader of the partition at the time of the snapshot.
    #[kafka(versions = "0+", tag = 0, tagged_versions = "0+")]
    pub current_leader: LeaderIdAndEpoch,
    /// The total size of the snapshot.
    #[kafka(versions = "0+")]
    pub size: i64,
    /// The starting byte position within the snapshot included in the Bytes field.
    #[kafka(versions = "0+")]
    pub position: i64,
    /// Snapshot data in records format which may not be aligned on an offset boundary.
    #[kafka(versions = "0+")]
    pub unaligned_records: RecordBatch,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicSnapshot {
    /// The name of the topic to fetch.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The partitions to fetch.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionSnapshot>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    #[kafka(versions = "1+", map_key)]
    pub node_id: i32,
    /// The node's hostname.
    #[kafka(versions = "1+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "1+")]
    pub port: u16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 59,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct FetchSnapshotResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The topics to fetch.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicSnapshot>,
    /// Endpoints for all current-leaders enumerated in PartitionSnapshot.
    #[kafka(versions = "1+", tag = 0, tagged_versions = "1+")]
    pub node_endpoints: Vec<NodeEndpoint>,
}
