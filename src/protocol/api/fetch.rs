//! Fetch API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 1

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// FetchRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 1, valid_versions = "4-18", flexible_versions = "12+")]
pub struct FetchRequest {
    #[kafka(versions = "12+")]
    pub cluster_id: String,
    #[kafka(versions = "0-14")]
    pub replica_id: i32,
    #[kafka(versions = "15+")]
    pub replica_state: FetchRequestReplicaState,
    #[kafka(versions = "0+")]
    pub max_wait_ms: i32,
    #[kafka(versions = "0+")]
    pub min_bytes: i32,
    #[kafka(versions = "3+")]
    pub max_bytes: i32,
    #[kafka(versions = "4+")]
    pub isolation_level: i8,
    #[kafka(versions = "7+")]
    pub session_id: i32,
    #[kafka(versions = "7+")]
    pub session_epoch: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<FetchRequestFetchTopic>,
    #[kafka(versions = "7+")]
    pub forgotten_topics_data: Vec<FetchRequestForgottenTopic>,
    #[kafka(versions = "11+")]
    pub rack_id: String,
}


/// FetchRequestReplicaState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchRequestReplicaState {
    #[kafka(versions = "15+")]
    pub replica_id: i32,
    #[kafka(versions = "15+")]
    pub replica_epoch: i64,
}

/// FetchRequestFetchTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchRequestFetchTopic {
    #[kafka(versions = "0-12")]
    pub topic: String,
    #[kafka(versions = "13+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<FetchRequestFetchPartition>,
}

/// FetchRequestFetchPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchRequestFetchPartition {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "9+")]
    pub current_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub fetch_offset: i64,
    #[kafka(versions = "12+")]
    pub last_fetched_epoch: i32,
    #[kafka(versions = "5+")]
    pub log_start_offset: i64,
    #[kafka(versions = "0+")]
    pub partition_max_bytes: i32,
    #[kafka(versions = "17+")]
    pub replica_directory_id: Uuid,
    #[kafka(versions = "18+")]
    pub high_watermark: i64,
}

/// FetchRequestForgottenTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchRequestForgottenTopic {
    #[kafka(versions = "7-12")]
    pub topic: String,
    #[kafka(versions = "13+")]
    pub topic_id: Uuid,
    #[kafka(versions = "7+")]
    pub partitions: Vec<i32>,
}
/// FetchResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 1, valid_versions = "4-18", flexible_versions = "12+")]
pub struct FetchResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "7+")]
    pub error_code: i16,
    #[kafka(versions = "7+")]
    pub session_id: i32,
    #[kafka(versions = "0+")]
    pub responses: Vec<FetchResponseFetchableTopicResponse>,
    #[kafka(versions = "16+")]
    pub node_endpoints: Vec<FetchResponseNodeEndpoint>,
}


/// FetchResponseFetchableTopicResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponseFetchableTopicResponse {
    #[kafka(versions = "0-12")]
    pub topic: String,
    #[kafka(versions = "13+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<FetchResponsePartitionData>,
}

/// FetchResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub high_watermark: i64,
    #[kafka(versions = "4+")]
    pub last_stable_offset: i64,
    #[kafka(versions = "5+")]
    pub log_start_offset: i64,
    #[kafka(versions = "12+")]
    pub diverging_epoch: FetchResponseEpochEndOffset,
    #[kafka(versions = "12+")]
    pub current_leader: FetchResponseLeaderIdAndEpoch,
    #[kafka(versions = "12+")]
    pub snapshot_id: FetchResponseSnapshotId,
    #[kafka(versions = "4+")]
    pub aborted_transactions: Vec<FetchResponseAbortedTransaction>,
    #[kafka(versions = "11+")]
    pub preferred_read_replica: i32,
    #[kafka(versions = "0+")]
    pub records: Vec<u8>,
}

/// FetchResponseEpochEndOffset
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponseEpochEndOffset {
    #[kafka(versions = "12+")]
    pub epoch: i32,
    #[kafka(versions = "12+")]
    pub end_offset: i64,
}

/// FetchResponseLeaderIdAndEpoch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponseLeaderIdAndEpoch {
    #[kafka(versions = "12+")]
    pub leader_id: i32,
    #[kafka(versions = "12+")]
    pub leader_epoch: i32,
}

/// FetchResponseSnapshotId
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponseSnapshotId {
    #[kafka(versions = "0+")]
    pub end_offset: i64,
    #[kafka(versions = "0+")]
    pub epoch: i32,
}

/// FetchResponseAbortedTransaction
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponseAbortedTransaction {
    #[kafka(versions = "4+")]
    pub producer_id: i64,
    #[kafka(versions = "4+")]
    pub first_offset: i64,
}

/// FetchResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct FetchResponseNodeEndpoint {
    #[kafka(versions = "16+")]
    pub node_id: i32,
    #[kafka(versions = "16+")]
    pub host: String,
    #[kafka(versions = "16+")]
    pub port: i32,
    #[kafka(versions = "16+")]
    pub rack: String,
}
