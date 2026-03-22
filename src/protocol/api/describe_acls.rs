//! DescribeAcls API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 29

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeAclsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 29, valid_versions = "1-3", flexible_versions = "2+")]
pub struct DescribeAclsRequest {
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

/// DescribeAclsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 29, valid_versions = "1-3", flexible_versions = "2+")]
pub struct DescribeAclsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub resources: Vec<DescribeAclsResponseDescribeAclsResource>,
}


/// DescribeAclsResponseDescribeAclsResource
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeAclsResponseDescribeAclsResource {
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "1+")]
    pub pattern_type: i8,
    #[kafka(versions = "0+")]
    pub acls: Vec<DescribeAclsResponseAclDescription>,
}

/// DescribeAclsResponseAclDescription
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeAclsResponseAclDescription {
    #[kafka(versions = "0+")]
    pub principal: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub operation: i8,
    #[kafka(versions = "0+")]
    pub permission_type: i8,
}
