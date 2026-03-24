//! Auto-generated from Kafka protocol
//! Message: DescribeQuorumRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partitions to describe.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 55, msg_type = "request", valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeQuorumRequest {
    /// The topics to describe.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}

