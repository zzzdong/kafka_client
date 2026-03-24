//! Auto-generated from Kafka protocol
//! Message: DescribeLogDirsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribableLogDirTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic: String,
    /// The partition indexes.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 35, msg_type = "request", valid_versions = "1-4", flexible_versions = "2+")]
pub struct DescribeLogDirsRequest {
    /// Each topic that we want to describe log directories for, or null for all topics.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub topics: Option<Vec<DescribableLogDirTopic>>,
}

