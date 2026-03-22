//! DeleteShareGroupState API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 86

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// DeleteShareGroupStateRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 86, valid_versions = "0", flexible_versions = "0+")]
pub struct DeleteShareGroupStateRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteShareGroupStateRequestDeleteStateData>,
}


/// DeleteShareGroupStateRequestDeleteStateData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteShareGroupStateRequestDeleteStateData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DeleteShareGroupStateRequestPartitionData>,
}

/// DeleteShareGroupStateRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteShareGroupStateRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition: i32,
}
/// DeleteShareGroupStateResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 86, valid_versions = "0", flexible_versions = "0+")]
pub struct DeleteShareGroupStateResponse {
    #[kafka(versions = "0+")]
    pub results: Vec<DeleteShareGroupStateResponseDeleteStateResult>,
}


/// DeleteShareGroupStateResponseDeleteStateResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteShareGroupStateResponseDeleteStateResult {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DeleteShareGroupStateResponsePartitionResult>,
}

/// DeleteShareGroupStateResponsePartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteShareGroupStateResponsePartitionResult {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
