//! ListPartitionReassignments API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 46

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ListPartitionReassignmentsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 46, valid_versions = "0", flexible_versions = "0+")]
pub struct ListPartitionReassignmentsRequest {
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<ListPartitionReassignmentsRequestListPartitionReassignmentsTopics>,
}


/// ListPartitionReassignmentsRequestListPartitionReassignmentsTopics
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListPartitionReassignmentsRequestListPartitionReassignmentsTopics {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partition_indexes: Vec<i32>,
}
/// ListPartitionReassignmentsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 46, valid_versions = "0", flexible_versions = "0+")]
pub struct ListPartitionReassignmentsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<ListPartitionReassignmentsResponseOngoingTopicReassignment>,
}


/// ListPartitionReassignmentsResponseOngoingTopicReassignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListPartitionReassignmentsResponseOngoingTopicReassignment {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ListPartitionReassignmentsResponseOngoingPartitionReassignment>,
}

/// ListPartitionReassignmentsResponseOngoingPartitionReassignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListPartitionReassignmentsResponseOngoingPartitionReassignment {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub replicas: Vec<i32>,
    #[kafka(versions = "0+")]
    pub adding_replicas: Vec<i32>,
    #[kafka(versions = "0+")]
    pub removing_replicas: Vec<i32>,
}
