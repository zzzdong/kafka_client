//! AlterPartitionReassignments API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 45

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AlterPartitionReassignmentsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 45, valid_versions = "0-1", flexible_versions = "0+")]
pub struct AlterPartitionReassignmentsRequest {
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "1+")]
    pub allow_replication_factor_change: bool,
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterPartitionReassignmentsRequestReassignableTopic>,
}


/// AlterPartitionReassignmentsRequestReassignableTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionReassignmentsRequestReassignableTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterPartitionReassignmentsRequestReassignablePartition>,
}

/// AlterPartitionReassignmentsRequestReassignablePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionReassignmentsRequestReassignablePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub replicas: Vec<i32>,
}
/// AlterPartitionReassignmentsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 45, valid_versions = "0-1", flexible_versions = "0+")]
pub struct AlterPartitionReassignmentsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "1+")]
    pub allow_replication_factor_change: bool,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub responses: Vec<AlterPartitionReassignmentsResponseReassignableTopicResponse>,
}


/// AlterPartitionReassignmentsResponseReassignableTopicResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionReassignmentsResponseReassignableTopicResponse {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterPartitionReassignmentsResponseReassignablePartitionResponse>,
}

/// AlterPartitionReassignmentsResponseReassignablePartitionResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionReassignmentsResponseReassignablePartitionResponse {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
