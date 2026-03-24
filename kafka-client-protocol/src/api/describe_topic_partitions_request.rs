//! Auto-generated from Kafka protocol
//! Message: DescribeTopicPartitionsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicRequest {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partition index to start with.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 75, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeTopicPartitionsRequest {
    /// The topics to fetch details for.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicRequest>,
    /// The maximum number of partitions included in the response.
    #[kafka(versions = "0+", default = 2000)]
    pub response_partition_limit: i32,
    /// The first topic and partition index to fetch details for.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub cursor: Option<Cursor>,
}

