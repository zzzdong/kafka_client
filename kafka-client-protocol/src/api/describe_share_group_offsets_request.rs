//! Auto-generated from Kafka protocol
//! Message: DescribeShareGroupOffsetsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsRequestTopic {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsRequestGroup {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The topics to describe offsets for, or null for all topic-partitions.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 90, msg_type = "request", valid_versions = "0-1", flexible_versions = "0+")]
pub struct DescribeShareGroupOffsetsRequest {
    /// The groups to describe offsets for.
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribeShareGroupOffsetsRequestGroup>,
}

