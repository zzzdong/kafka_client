//! Auto-generated from Kafka protocol
//! Message: FetchSnapshotRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the snapshot.
    #[kafka(versions = "0+")]
    pub end_offset: i64,
    /// The epoch of the snapshot.
    #[kafka(versions = "0+")]
    pub epoch: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionSnapshot {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    #[kafka(versions = "0+")]
    pub current_leader_epoch: i32,
    /// The snapshot endOffset and epoch to fetch.
    #[kafka(versions = "0+")]
    pub snapshot_id: SnapshotId,
    /// The byte position within the snapshot to start fetching from.
    #[kafka(versions = "0+")]
    pub position: i64,
    /// The directory id of the follower fetching.
    #[kafka(
        versions = "1+",
        nullable_versions = "1+",
        tag = 0,
        tagged_versions = "1+"
    )]
    pub replica_directory_id: Option<Uuid>,
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
#[kafka(
    api_key = 59,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct FetchSnapshotRequest {
    /// The clusterId if known, this is used to validate metadata fetches prior to broker registration.
    #[kafka(versions = "0+", nullable_versions = "0+", tag = 0, tagged_versions = "0+", default = None)]
    pub cluster_id: Option<String>,
    /// The broker ID of the follower.
    #[kafka(versions = "0+", default = -1)]
    pub replica_id: i32,
    /// The maximum bytes to fetch from all of the snapshots.
    #[kafka(versions = "0+", default = 0x7fffffff)]
    pub max_bytes: i32,
    /// The topics to fetch.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicSnapshot>,
}
