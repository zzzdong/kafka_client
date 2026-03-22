//! AllocateProducerIds API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 67

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AllocateProducerIdsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 67, valid_versions = "0", flexible_versions = "0+")]
pub struct AllocateProducerIdsRequest {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    #[kafka(versions = "0+")]
    pub broker_epoch: i64,
}

/// AllocateProducerIdsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 67, valid_versions = "0", flexible_versions = "0+")]
pub struct AllocateProducerIdsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub producer_id_start: i64,
    #[kafka(versions = "0+")]
    pub producer_id_len: i32,
}

