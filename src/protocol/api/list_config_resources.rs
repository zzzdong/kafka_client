//! ListConfigResources API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 74

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ListConfigResourcesRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 74, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ListConfigResourcesRequest {
    #[kafka(versions = "1+")]
    pub resource_types: Vec<i8>,
}

/// ListConfigResourcesResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 74, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ListConfigResourcesResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub config_resources: Vec<ListConfigResourcesResponseConfigResource>,
}


/// ListConfigResourcesResponseConfigResource
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ListConfigResourcesResponseConfigResource {
    #[kafka(versions = "0+")]
    pub resource_name: String,
    #[kafka(versions = "1+")]
    pub resource_type: i8,
}
