//! FetchSnapshot API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 59

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// FetchSnapshotRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 59, valid_versions = "0-1", flexible_versions = "0+")]
pub struct FetchSnapshotRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    #[kafka(versions = "0+")]
    pub max_bytes: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<FetchSnapshotRequestTopicSnapshot>,
}


/// FetchSnapshotRequestTopicSnapshot
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotRequestTopicSnapshot {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<FetchSnapshotRequestPartitionSnapshot>,
}

/// FetchSnapshotRequestPartitionSnapshot
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotRequestPartitionSnapshot {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub current_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub snapshot_id: FetchSnapshotRequestSnapshotId,
    #[kafka(versions = "0+")]
    pub position: i64,
    #[kafka(versions = "1+")]
    pub replica_directory_id: Uuid,
}

/// FetchSnapshotRequestSnapshotId
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotRequestSnapshotId {
    #[kafka(versions = "0+")]
    pub end_offset: i64,
    #[kafka(versions = "0+")]
    pub epoch: i32,
}
/// FetchSnapshotResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 59, valid_versions = "0-1", flexible_versions = "0+")]
pub struct FetchSnapshotResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub topics: Vec<FetchSnapshotResponseTopicSnapshot>,
    #[kafka(versions = "1+")]
    pub node_endpoints: Vec<FetchSnapshotResponseNodeEndpoint>,
}


/// FetchSnapshotResponseTopicSnapshot
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotResponseTopicSnapshot {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<FetchSnapshotResponsePartitionSnapshot>,
}

/// FetchSnapshotResponsePartitionSnapshot
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotResponsePartitionSnapshot {
    #[kafka(versions = "0+")]
    pub index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub snapshot_id: FetchSnapshotResponseSnapshotId,
    #[kafka(versions = "0+")]
    pub current_leader: FetchSnapshotResponseLeaderIdAndEpoch,
    #[kafka(versions = "0+")]
    pub size: i64,
    #[kafka(versions = "0+")]
    pub position: i64,
    #[kafka(versions = "0+")]
    pub unaligned_records: Vec<u8>,
}

/// FetchSnapshotResponseSnapshotId
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotResponseSnapshotId {
    #[kafka(versions = "0+")]
    pub end_offset: i64,
    #[kafka(versions = "0+")]
    pub epoch: i32,
}

/// FetchSnapshotResponseLeaderIdAndEpoch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotResponseLeaderIdAndEpoch {
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

/// FetchSnapshotResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchSnapshotResponseNodeEndpoint {
    #[kafka(versions = "1+")]
    pub node_id: i32,
    #[kafka(versions = "1+")]
    pub host: String,
    #[kafka(versions = "1+")]
    pub port: i16,
}
