//! DescribeClientQuotas API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 48

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeClientQuotasRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 48, valid_versions = "0-1", flexible_versions = "1+")]
pub struct DescribeClientQuotasRequest {
    #[kafka(versions = "0+")]
    pub components: Vec<DescribeClientQuotasRequestComponentData>,
    #[kafka(versions = "0+")]
    pub strict: bool,
}


/// DescribeClientQuotasRequestComponentData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeClientQuotasRequestComponentData {
    #[kafka(versions = "0+")]
    pub entity_type: String,
    #[kafka(versions = "0+")]
    pub match_type: i8,
    #[kafka(versions = "0+")]
    pub match: String,
}
/// DescribeClientQuotasResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 48, valid_versions = "0-1", flexible_versions = "1+")]
pub struct DescribeClientQuotasResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub entries: Vec<DescribeClientQuotasResponseEntryData>,
}


/// DescribeClientQuotasResponseEntryData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeClientQuotasResponseEntryData {
    #[kafka(versions = "0+")]
    pub entity: Vec<DescribeClientQuotasResponseEntityData>,
    #[kafka(versions = "0+")]
    pub values: Vec<DescribeClientQuotasResponseValueData>,
}

/// DescribeClientQuotasResponseEntityData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeClientQuotasResponseEntityData {
    #[kafka(versions = "0+")]
    pub entity_type: String,
    #[kafka(versions = "0+")]
    pub entity_name: String,
}

/// DescribeClientQuotasResponseValueData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeClientQuotasResponseValueData {
    #[kafka(versions = "0+")]
    pub key: String,
    #[kafka(versions = "0+")]
    pub value: Float64,
}
