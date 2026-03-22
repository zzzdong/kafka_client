//! CreateAcls API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 30

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// CreateAclsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 30, valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateAclsRequest {
    #[kafka(versions = "0+")]
    pub creations: Vec<CreateAclsRequestAclCreation>,
}


/// CreateAclsRequestAclCreation
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateAclsRequestAclCreation {
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "1+")]
    pub resource_pattern_type: i8,
    #[kafka(versions = "0+")]
    pub principal: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub operation: i8,
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}
/// CreateAclsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 30, valid_versions = "1-3", flexible_versions = "2+")]
pub struct CreateAclsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub results: Vec<CreateAclsResponseAclCreationResult>,
}


/// CreateAclsResponseAclCreationResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct CreateAclsResponseAclCreationResult {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
