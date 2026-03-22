//! DeleteGroups API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 42

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DeleteGroupsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 42, valid_versions = "0-2", flexible_versions = "2+")]
pub struct DeleteGroupsRequest {
    #[kafka(versions = "0+")]
    pub groups_names: Vec<String>,
}

/// DeleteGroupsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 42, valid_versions = "0-2", flexible_versions = "2+")]
pub struct DeleteGroupsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub results: Vec<DeleteGroupsResponseDeletableGroupResult>,
}


/// DeleteGroupsResponseDeletableGroupResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteGroupsResponseDeletableGroupResult {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
