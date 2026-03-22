//! DescribeTopicPartitions API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 75

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// DescribeTopicPartitionsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 75, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeTopicPartitionsRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeTopicPartitionsRequestTopicRequest>,
    #[kafka(versions = "0+")]
    pub response_partition_limit: i32,
    #[kafka(versions = "0+")]
    pub cursor: DescribeTopicPartitionsRequestCursor,
}


/// DescribeTopicPartitionsRequestTopicRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTopicPartitionsRequestTopicRequest {
    #[kafka(versions = "0+")]
    pub name: String,
}

/// DescribeTopicPartitionsRequestCursor
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTopicPartitionsRequestCursor {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}
/// DescribeTopicPartitionsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 75, valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeTopicPartitionsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic>,
    #[kafka(versions = "0+")]
    pub next_cursor: DescribeTopicPartitionsResponseCursor,
}


/// DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub is_internal: bool,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition>,
    #[kafka(versions = "0+")]
    pub topic_authorized_operations: i32,
}

/// DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub replica_nodes: Vec<i32>,
    #[kafka(versions = "0+")]
    pub isr_nodes: Vec<i32>,
    #[kafka(versions = "0+")]
    pub eligible_leader_replicas: Vec<i32>,
    #[kafka(versions = "0+")]
    pub last_known_elr: Vec<i32>,
    #[kafka(versions = "0+")]
    pub offline_replicas: Vec<i32>,
}

/// DescribeTopicPartitionsResponseCursor
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeTopicPartitionsResponseCursor {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}
