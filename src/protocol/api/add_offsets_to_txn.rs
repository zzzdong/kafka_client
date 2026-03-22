//! AddOffsetsToTxn API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 25

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AddOffsetsToTxnRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 25, valid_versions = "0-4", flexible_versions = "3+")]
pub struct AddOffsetsToTxnRequest {
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    #[kafka(versions = "0+")]
    pub group_id: String,
}

/// AddOffsetsToTxnResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 25, valid_versions = "0-4", flexible_versions = "3+")]
pub struct AddOffsetsToTxnResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}

