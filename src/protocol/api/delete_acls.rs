//! DeleteAcls API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 31

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DeleteAclsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 31, valid_versions = "1-3", flexible_versions = "2+")]
pub struct DeleteAclsRequest {
    #[kafka(versions = "0+")]
    pub filters: Vec<DeleteAclsRequestDeleteAclsFilter>,
}


/// DeleteAclsRequestDeleteAclsFilter
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteAclsRequestDeleteAclsFilter {
    #[kafka(versions = "0+")]
    pub resource_type_filter: i8,
    #[kafka(versions = "0+")]
    pub resource_name_filter: String,
    #[kafka(versions = "1+")]
    pub pattern_type_filter: i8,
    #[kafka(versions = "0+")]
    pub principal_filter: String,
    #[kafka(versions = "0+")]
    pub host_filter: String,
    #[kafka(versions = "0+")]
    pub operation: i8,
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}
/// DeleteAclsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 31, valid_versions = "1-3", flexible_versions = "2+")]
pub struct DeleteAclsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub filter_results: Vec<DeleteAclsResponseDeleteAclsFilterResult>,
}


/// DeleteAclsResponseDeleteAclsFilterResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteAclsResponseDeleteAclsFilterResult {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub matching_acls: Vec<DeleteAclsResponseDeleteAclsMatchingAcl>,
}

/// DeleteAclsResponseDeleteAclsMatchingAcl
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DeleteAclsResponseDeleteAclsMatchingAcl {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "1+")]
    pub pattern_type: i8,
    #[kafka(versions = "0+")]
    pub principal: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub operation: i8,
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}
