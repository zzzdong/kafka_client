//! DeleteShareGroupOffsets API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 92

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// DeleteShareGroupOffsetsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 92, valid_versions = "0", flexible_versions = "0+")]
pub struct DeleteShareGroupOffsetsRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteShareGroupOffsetsRequestDeleteShareGroupOffsetsRequestTopic>,
}


/// DeleteShareGroupOffsetsRequestDeleteShareGroupOffsetsRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteShareGroupOffsetsRequestDeleteShareGroupOffsetsRequestTopic {
    #[kafka(versions = "0+")]
    pub topic_name: String,
}
/// DeleteShareGroupOffsetsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 92, valid_versions = "0", flexible_versions = "0+")]
pub struct DeleteShareGroupOffsetsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub responses: Vec<DeleteShareGroupOffsetsResponseDeleteShareGroupOffsetsResponseTopic>,
}


/// DeleteShareGroupOffsetsResponseDeleteShareGroupOffsetsResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteShareGroupOffsetsResponseDeleteShareGroupOffsetsResponseTopic {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
