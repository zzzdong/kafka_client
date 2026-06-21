//! Auto-generated from Kafka protocol
//! Message: DescribeProducersRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicRequest {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The indexes of the partitions to list producers for.
    #[kafka(versions = "0+")]
    pub partition_indexes: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 61,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct DescribeProducersRequest {
    /// The topics to list producers for.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicRequest>,
}
