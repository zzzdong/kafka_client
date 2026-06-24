//! Auto-generated from Kafka protocol
//! Message: DescribeShareGroupOffsetsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponsePartition {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The share-partition start offset.
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    /// The leader epoch of the partition.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// The share-partition lag.
    #[kafka(versions = "1+", default = -1)]
    pub lag: i64,
    /// The partition-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The partition-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponseTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The unique topic ID.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponseGroup {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The results for each topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeShareGroupOffsetsResponseTopic>,
    /// The group-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The group-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 90,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct DescribeShareGroupOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The results for each group.
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribeShareGroupOffsetsResponseGroup>,
}
