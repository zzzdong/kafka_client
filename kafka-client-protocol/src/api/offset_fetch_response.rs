//! Auto-generated from Kafka protocol
//! Message: OffsetFetchResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchResponsePartition {
    /// The partition index.
    #[kafka(versions = "0-7")]
    pub partition_index: i32,
    /// The committed message offset.
    #[kafka(versions = "0-7")]
    pub committed_offset: i64,
    /// The leader epoch.
    #[kafka(versions = "5-7", nullable_versions = "5-7", default = -1)]
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    #[kafka(versions = "0-7", nullable_versions = "0-7")]
    pub metadata: Option<String>,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0-7")]
    pub error_code: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchResponseTopic {
    /// The topic name.
    #[kafka(versions = "0-7")]
    pub name: String,
    /// The responses per partition.
    #[kafka(versions = "0-7")]
    pub partitions: Vec<OffsetFetchResponsePartition>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchResponsePartitions {
    /// The partition index.
    #[kafka(versions = "8+")]
    pub partition_index: i32,
    /// The committed message offset.
    #[kafka(versions = "8+")]
    pub committed_offset: i64,
    /// The leader epoch.
    #[kafka(versions = "8+", nullable_versions = "8+", default = -1)]
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    #[kafka(versions = "8+", nullable_versions = "8+")]
    pub metadata: Option<String>,
    /// The partition-level error code, or 0 if there was no error.
    #[kafka(versions = "8+")]
    pub error_code: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchResponseTopics {
    /// The topic name.
    #[kafka(versions = "8-9", nullable_versions = "8-9")]
    pub name: Option<String>,
    /// The topic ID.
    #[kafka(versions = "10+", nullable_versions = "10+")]
    pub topic_id: Option<Uuid>,
    /// The responses per partition.
    #[kafka(versions = "8+")]
    pub partitions: Vec<OffsetFetchResponsePartitions>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchResponseGroup {
    /// The group ID.
    #[kafka(versions = "8+")]
    pub group_id: String,
    /// The responses per topic.
    #[kafka(versions = "8+")]
    pub topics: Vec<OffsetFetchResponseTopics>,
    /// The group-level error code, or 0 if there was no error.
    #[kafka(versions = "8+", default = 0)]
    pub error_code: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 9, msg_type = "response", valid_versions = "1-10", flexible_versions = "6+")]
pub struct OffsetFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "3+", nullable_versions = "3+")]
    pub throttle_time_ms: i32,
    /// The responses per topic.
    #[kafka(versions = "0-7")]
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "2-7", nullable_versions = "2-7", default = 0)]
    pub error_code: i16,
    /// The responses per group id.
    #[kafka(versions = "8+")]
    pub groups: Vec<OffsetFetchResponseGroup>,
}

