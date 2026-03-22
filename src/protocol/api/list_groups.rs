//! ListGroups API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 16

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ListGroupsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 16, valid_versions = "0-5", flexible_versions = "3+")]
pub struct ListGroupsRequest {
    #[kafka(versions = "4+")]
    pub states_filter: Vec<String>,
    #[kafka(versions = "5+")]
    pub types_filter: Vec<String>,
}

/// ListGroupsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 16, valid_versions = "0-5", flexible_versions = "3+")]
pub struct ListGroupsResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub groups: Vec<ListGroupsResponseListedGroup>,
}


/// ListGroupsResponseListedGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListGroupsResponseListedGroup {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub protocol_type: String,
    #[kafka(versions = "4+")]
    pub group_state: String,
    #[kafka(versions = "5+")]
    pub group_type: String,
}
