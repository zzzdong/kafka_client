//! Auto-generated from Kafka protocol
//! Message: VoteRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The epoch of the voter sending the request
    #[kafka(versions = "0+")]
    pub replica_epoch: i32,
    /// The replica id of the voter sending the request
    #[kafka(versions = "0+")]
    pub replica_id: i32,
    /// The directory id of the voter sending the request
    #[kafka(versions = "1+")]
    pub replica_directory_id: Uuid,
    /// The directory id of the voter receiving the request
    #[kafka(versions = "1+")]
    pub voter_directory_id: Uuid,
    /// The epoch of the last record written to the metadata log.
    #[kafka(versions = "0+")]
    pub last_offset_epoch: i32,
    /// The log end offset of the metadata log of the voter sending the request.
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    /// Whether the request is a PreVote request (not persisted) or not.
    #[kafka(versions = "2+")]
    pub pre_vote: bool,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partition data.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 52,
    msg_type = "request",
    valid_versions = "0-2",
    flexible_versions = "0+"
)]
pub struct VoteRequest {
    /// The cluster id.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub cluster_id: Option<String>,
    /// The replica id of the voter receiving the request.
    #[kafka(versions = "1+", default = -1)]
    pub voter_id: i32,
    /// The topic data.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}
