//! DeleteTopics API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 20

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// DeleteTopicsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 20, valid_versions = "1-6", flexible_versions = "4+")]
pub struct DeleteTopicsRequest {
    #[kafka(versions = "6+")]
    pub topics: Vec<DeleteTopicsRequestDeleteTopicState>,
    #[kafka(versions = "0-5")]
    pub topic_names: Vec<String>,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
}


/// DeleteTopicsRequestDeleteTopicState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteTopicsRequestDeleteTopicState {
    #[kafka(versions = "6+")]
    pub name: String,
    #[kafka(versions = "6+")]
    pub topic_id: Uuid,
}
/// DeleteTopicsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 20, valid_versions = "1-6", flexible_versions = "4+")]
pub struct DeleteTopicsResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub responses: Vec<DeleteTopicsResponseDeletableTopicResult>,
}


/// DeleteTopicsResponseDeletableTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteTopicsResponseDeletableTopicResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "6+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "5+")]
    pub error_message: String,
}
