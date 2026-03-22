//! AddPartitionsToTxn API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 24

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AddPartitionsToTxnRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 24, valid_versions = "0-5", flexible_versions = "3+")]
pub struct AddPartitionsToTxnRequest {
    #[kafka(versions = "4+")]
    pub transactions: Vec<AddPartitionsToTxnRequestAddPartitionsToTxnTransaction>,
    #[kafka(versions = "0-3")]
    pub v3_and_below_transactional_id: String,
    #[kafka(versions = "0-3")]
    pub v3_and_below_producer_id: i64,
    #[kafka(versions = "0-3")]
    pub v3_and_below_producer_epoch: i16,
    #[kafka(versions = "0-3")]
    pub v3_and_below_topics: Vec<AddPartitionsToTxnTopic>,
}


/// AddPartitionsToTxnRequestAddPartitionsToTxnTransaction
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AddPartitionsToTxnRequestAddPartitionsToTxnTransaction {
    #[kafka(versions = "4+")]
    pub transactional_id: String,
    #[kafka(versions = "4+")]
    pub producer_id: i64,
    #[kafka(versions = "4+")]
    pub producer_epoch: i16,
    #[kafka(versions = "4+")]
    pub verify_only: bool,
    #[kafka(versions = "4+")]
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

/// AddPartitionsToTxnRequestAddPartitionsToTxnTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AddPartitionsToTxnRequestAddPartitionsToTxnTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// AddPartitionsToTxnResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 24, valid_versions = "0-5", flexible_versions = "3+")]
pub struct AddPartitionsToTxnResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "4+")]
    pub error_code: i16,
    #[kafka(versions = "4+")]
    pub results_by_transaction: Vec<AddPartitionsToTxnResponseAddPartitionsToTxnResult>,
    #[kafka(versions = "0-3")]
    pub results_by_topic_v3_and_below: Vec<AddPartitionsToTxnTopicResult>,
}


/// AddPartitionsToTxnResponseAddPartitionsToTxnResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AddPartitionsToTxnResponseAddPartitionsToTxnResult {
    #[kafka(versions = "4+")]
    pub transactional_id: String,
    #[kafka(versions = "4+")]
    pub topic_results: Vec<AddPartitionsToTxnTopicResult>,
}

/// AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub results_by_partition: Vec<AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult>,
}

/// AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub partition_error_code: i16,
}
