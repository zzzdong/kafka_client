//! ElectLeaders API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 43

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ElectLeadersRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 43, valid_versions = "0-2", flexible_versions = "2+")]
pub struct ElectLeadersRequest {
    #[kafka(versions = "1+")]
    pub election_type: i8,
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<ElectLeadersRequestTopicPartitions>,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
}


/// ElectLeadersRequestTopicPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ElectLeadersRequestTopicPartitions {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// ElectLeadersResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 43, valid_versions = "0-2", flexible_versions = "2+")]
pub struct ElectLeadersResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "1+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub replica_election_results: Vec<ElectLeadersResponseReplicaElectionResult>,
}


/// ElectLeadersResponseReplicaElectionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ElectLeadersResponseReplicaElectionResult {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partition_result: Vec<ElectLeadersResponsePartitionResult>,
}

/// ElectLeadersResponsePartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ElectLeadersResponsePartitionResult {
    #[kafka(versions = "0+")]
    pub partition_id: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
