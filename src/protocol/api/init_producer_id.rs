//! InitProducerId API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 22

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// InitProducerIdRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 22, valid_versions = "0-6", flexible_versions = "2+")]
pub struct InitProducerIdRequest {
    #[kafka(versions = "0+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub transaction_timeout_ms: i32,
    #[kafka(versions = "3+")]
    pub producer_id: i64,
    #[kafka(versions = "3+")]
    pub producer_epoch: i16,
    #[kafka(versions = "6+")]
    pub enable2_pc: bool,
    #[kafka(versions = "6+")]
    pub keep_prepared_txn: bool,
}

/// InitProducerIdResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 22, valid_versions = "0-6", flexible_versions = "2+")]
pub struct InitProducerIdResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    #[kafka(versions = "0+")]
    pub producer_epoch: i16,
    #[kafka(versions = "6+")]
    pub ongoing_txn_producer_id: i64,
    #[kafka(versions = "6+")]
    pub ongoing_txn_producer_epoch: i16,
}

