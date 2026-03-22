//! AlterConfigs API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 33

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AlterConfigsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 33, valid_versions = "0-2", flexible_versions = "2+")]
pub struct AlterConfigsRequest {
    #[kafka(versions = "0+")]
    pub resources: Vec<AlterConfigsRequestAlterConfigsResource>,
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}


/// AlterConfigsRequestAlterConfigsResource
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterConfigsRequestAlterConfigsResource {
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "0+")]
    pub configs: Vec<AlterConfigsRequestAlterableConfig>,
}

/// AlterConfigsRequestAlterableConfig
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterConfigsRequestAlterableConfig {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub value: String,
}
/// AlterConfigsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 33, valid_versions = "0-2", flexible_versions = "2+")]
pub struct AlterConfigsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub responses: Vec<AlterConfigsResponseAlterConfigsResourceResponse>,
}


/// AlterConfigsResponseAlterConfigsResourceResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterConfigsResponseAlterConfigsResourceResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub resource_type: i8,
    #[kafka(versions = "0+")]
    pub resource_name: String,
}
