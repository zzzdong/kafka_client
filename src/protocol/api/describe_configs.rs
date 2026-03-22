//! DescribeConfigs API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 32

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeConfigsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 32, valid_versions = "1-4", flexible_versions = "4+")]
pub struct DescribeConfigsRequest {
    #[kafka(versions = "0+")]
    pub resources: Vec<DescribeConfigsRequestDescribeConfigsResource>,
    #[kafka(versions = "1+")]
    pub include_synonyms: bool,
    #[kafka(versions = "3+")]
    pub include_documentation: bool,
}


/// DescribeConfigsRequestDescribeConfigsResource
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeConfigsRequestDescribeConfigsResource {
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "0+")]
    pub configuration_keys: Vec<String>,
}
/// DescribeConfigsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 32, valid_versions = "1-4", flexible_versions = "4+")]
pub struct DescribeConfigsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub results: Vec<DescribeConfigsResponseDescribeConfigsResult>,
}


/// DescribeConfigsResponseDescribeConfigsResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeConfigsResponseDescribeConfigsResult {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "0+")]
    pub configs: Vec<DescribeConfigsResponseDescribeConfigsResourceResult>,
}

/// DescribeConfigsResponseDescribeConfigsResourceResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeConfigsResponseDescribeConfigsResourceResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub value: String,
    #[kafka(versions = "0+")]
    pub read_only: bool,
    #[kafka(versions = "1+")]
    pub config_source: i8,
    #[kafka(versions = "0+")]
    pub is_sensitive: bool,
    #[kafka(versions = "1+")]
    pub synonyms: Vec<DescribeConfigsResponseDescribeConfigsSynonym>,
    #[kafka(versions = "3+")]
    pub config_type: i8,
    #[kafka(versions = "3+")]
    pub documentation: String,
}

/// DescribeConfigsResponseDescribeConfigsSynonym
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeConfigsResponseDescribeConfigsSynonym {
    #[kafka(versions = "1+")]
    pub name: String,
    #[kafka(versions = "1+")]
    pub value: String,
    #[kafka(versions = "1+")]
    pub source: i8,
}
