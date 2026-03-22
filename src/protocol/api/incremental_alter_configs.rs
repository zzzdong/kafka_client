//! IncrementalAlterConfigs API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 44

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// IncrementalAlterConfigsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 44, valid_versions = "0-1", flexible_versions = "1+")]
pub struct IncrementalAlterConfigsRequest {
    #[kafka(versions = "0+")]
    pub resources: Vec<IncrementalAlterConfigsRequestAlterConfigsResource>,
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}


/// IncrementalAlterConfigsRequestAlterConfigsResource
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct IncrementalAlterConfigsRequestAlterConfigsResource {
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "0+")]
    pub configs: Vec<IncrementalAlterConfigsRequestAlterableConfig>,
}

/// IncrementalAlterConfigsRequestAlterableConfig
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct IncrementalAlterConfigsRequestAlterableConfig {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub config_operation: i8,
    #[kafka(versions = "0+")]
    pub value: String,
}
/// IncrementalAlterConfigsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 44, valid_versions = "0-1", flexible_versions = "1+")]
pub struct IncrementalAlterConfigsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub responses: Vec<IncrementalAlterConfigsResponseAlterConfigsResourceResponse>,
}


/// IncrementalAlterConfigsResponseAlterConfigsResourceResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct IncrementalAlterConfigsResponseAlterConfigsResourceResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
}
