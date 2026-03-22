//! CreatePartitions API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 37

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// CreatePartitionsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 37, valid_versions = "0-3", flexible_versions = "2+")]
pub struct CreatePartitionsRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<CreatePartitionsRequestCreatePartitionsTopic>,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}


/// CreatePartitionsRequestCreatePartitionsTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreatePartitionsRequestCreatePartitionsTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub count: i32,
    #[kafka(versions = "0+")]
    pub assignments: Vec<CreatePartitionsRequestCreatePartitionsAssignment>,
}

/// CreatePartitionsRequestCreatePartitionsAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreatePartitionsRequestCreatePartitionsAssignment {
    #[kafka(versions = "0+")]
    pub broker_ids: Vec<i32>,
}
/// CreatePartitionsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 37, valid_versions = "0-3", flexible_versions = "2+")]
pub struct CreatePartitionsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub results: Vec<CreatePartitionsResponseCreatePartitionsTopicResult>,
}


/// CreatePartitionsResponseCreatePartitionsTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreatePartitionsResponseCreatePartitionsTopicResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
