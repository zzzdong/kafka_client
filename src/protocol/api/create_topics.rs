//! CreateTopics API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 19

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// CreateTopicsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 19, valid_versions = "2-7", flexible_versions = "5+")]
pub struct CreateTopicsRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<CreateTopicsRequestCreatableTopic>,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "1+")]
    pub validate_only: bool,
}


/// CreateTopicsRequestCreatableTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateTopicsRequestCreatableTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub num_partitions: i32,
    #[kafka(versions = "0+")]
    pub replication_factor: i16,
    #[kafka(versions = "0+")]
    pub assignments: Vec<CreateTopicsRequestCreatableReplicaAssignment>,
    #[kafka(versions = "0+")]
    pub configs: Vec<CreateTopicsRequestCreatableTopicConfig>,
}

/// CreateTopicsRequestCreatableReplicaAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateTopicsRequestCreatableReplicaAssignment {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub broker_ids: Vec<i32>,
}

/// CreateTopicsRequestCreatableTopicConfig
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateTopicsRequestCreatableTopicConfig {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub value: String,
}
/// CreateTopicsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 19, valid_versions = "2-7", flexible_versions = "5+")]
pub struct CreateTopicsResponse {
    #[kafka(versions = "2+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<CreateTopicsResponseCreatableTopicResult>,
}


/// CreateTopicsResponseCreatableTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateTopicsResponseCreatableTopicResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "7+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "1+")]
    pub error_message: String,
    #[kafka(versions = "5+")]
    pub topic_config_error_code: i16,
    #[kafka(versions = "5+")]
    pub num_partitions: i32,
    #[kafka(versions = "5+")]
    pub replication_factor: i16,
    #[kafka(versions = "5+")]
    pub configs: Vec<CreateTopicsResponseCreatableTopicConfigs>,
}

/// CreateTopicsResponseCreatableTopicConfigs
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateTopicsResponseCreatableTopicConfigs {
    #[kafka(versions = "5+")]
    pub name: String,
    #[kafka(versions = "5+")]
    pub value: String,
    #[kafka(versions = "5+")]
    pub read_only: bool,
    #[kafka(versions = "5+")]
    pub config_source: i8,
    #[kafka(versions = "5+")]
    pub is_sensitive: bool,
}
