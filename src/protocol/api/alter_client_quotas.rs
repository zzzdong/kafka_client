//! AlterClientQuotas API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 49

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AlterClientQuotasRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 49, valid_versions = "0-1", flexible_versions = "1+")]
pub struct AlterClientQuotasRequest {
    #[kafka(versions = "0+")]
    pub entries: Vec<AlterClientQuotasRequestEntryData>,
    #[kafka(versions = "0+")]
    pub validate_only: bool,
}


/// AlterClientQuotasRequestEntryData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterClientQuotasRequestEntryData {
    #[kafka(versions = "0+")]
    pub entity: Vec<AlterClientQuotasRequestEntityData>,
    #[kafka(versions = "0+")]
    pub ops: Vec<AlterClientQuotasRequestOpData>,
}

/// AlterClientQuotasRequestEntityData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterClientQuotasRequestEntityData {
    #[kafka(versions = "0+")]
    pub entity_type: String,
    #[kafka(versions = "0+")]
    pub entity_name: String,
}

/// AlterClientQuotasRequestOpData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterClientQuotasRequestOpData {
    #[kafka(versions = "0+")]
    pub key: String,
    #[kafka(versions = "0+")]
    pub value: Float64,
    #[kafka(versions = "0+")]
    pub remove: bool,
}
/// AlterClientQuotasResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 49, valid_versions = "0-1", flexible_versions = "1+")]
pub struct AlterClientQuotasResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub entries: Vec<AlterClientQuotasResponseEntryData>,
}


/// AlterClientQuotasResponseEntryData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterClientQuotasResponseEntryData {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub entity: Vec<AlterClientQuotasResponseEntityData>,
}

/// AlterClientQuotasResponseEntityData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterClientQuotasResponseEntityData {
    #[kafka(versions = "0+")]
    pub entity_type: String,
    #[kafka(versions = "0+")]
    pub entity_name: String,
}
