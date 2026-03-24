//! Auto-generated from Kafka protocol
//! Message: FetchResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct EpochEndOffset {
    /// The largest epoch.
    #[kafka(versions = "12+", default = -1)]
    pub epoch: i32,
    /// The end offset of the epoch.
    #[kafka(versions = "12+", default = -1)]
    pub end_offset: i64,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "12+", default = -1)]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "12+", default = -1)]
    pub leader_epoch: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the epoch.
    #[kafka(versions = "0+", default = -1)]
    pub end_offset: i64,
    /// The largest epoch.
    #[kafka(versions = "0+", default = -1)]
    pub epoch: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    #[kafka(versions = "4+")]
    pub producer_id: i64,
    /// The first offset in the aborted transaction.
    #[kafka(versions = "4+")]
    pub first_offset: i64,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The current high water mark.
    #[kafka(versions = "0+")]
    pub high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
    #[kafka(versions = "4+", nullable_versions = "4+", default = -1)]
    pub last_stable_offset: i64,
    /// The current log start offset.
    #[kafka(versions = "5+", nullable_versions = "5+", default = -1)]
    pub log_start_offset: i64,
    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
    #[kafka(versions = "12+", tag = 0, tagged_versions = "12+")]
    pub diverging_epoch: EpochEndOffset,
    /// The current leader of the partition.
    #[kafka(versions = "12+", tag = 1, tagged_versions = "12+")]
    pub current_leader: LeaderIdAndEpoch,
    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
    #[kafka(versions = "12+", tag = 2, tagged_versions = "12+")]
    pub snapshot_id: SnapshotId,
    /// The aborted transactions.
    #[kafka(versions = "4+", nullable_versions = "4+")]
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    /// The preferred read replica for the consumer to use on its next fetch request.
    #[kafka(versions = "11+", default = -1)]
    pub preferred_read_replica: i32,
    /// The record data.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub records: Option<RecordBatch>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct FetchableTopicResponse {
    /// The topic name.
    #[kafka(versions = "0-12", nullable_versions = "0-12")]
    pub topic: Option<String>,
    /// The unique topic ID.
    #[kafka(versions = "13+", nullable_versions = "13+")]
    pub topic_id: Option<Uuid>,
    /// The topic partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    #[kafka(versions = "16+", map_key)]
    pub node_id: i32,
    /// The node's hostname.
    #[kafka(versions = "16+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "16+")]
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    #[kafka(versions = "16+", nullable_versions = "16+", default = None)]
    pub rack: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 1, msg_type = "response", valid_versions = "4-18", flexible_versions = "12+")]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "7+", nullable_versions = "7+")]
    pub error_code: i16,
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    #[kafka(versions = "7+", default = 0)]
    pub session_id: i32,
    /// The response topics.
    #[kafka(versions = "0+")]
    pub responses: Vec<FetchableTopicResponse>,
    /// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    #[kafka(versions = "16+", tag = 0, tagged_versions = "16+")]
    pub node_endpoints: Vec<NodeEndpoint>,
}

