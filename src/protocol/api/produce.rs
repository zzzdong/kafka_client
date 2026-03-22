//! Produce API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 0

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ProduceRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 0, valid_versions = "3-13", flexible_versions = "9+")]
pub struct ProduceRequest {
    #[kafka(versions = "3+")]
    pub transactional_id: String,
    #[kafka(versions = "0+")]
    pub acks: i16,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub topic_data: Vec<ProduceRequestTopicProduceData>,
}


/// ProduceRequestTopicProduceData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceRequestTopicProduceData {
    #[kafka(versions = "0-12")]
    pub name: String,
    #[kafka(versions = "13+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partition_data: Vec<ProduceRequestPartitionProduceData>,
}

/// ProduceRequestPartitionProduceData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceRequestPartitionProduceData {
    #[kafka(versions = "0+")]
    pub index: i32,
    #[kafka(versions = "0+")]
    pub records: Vec<u8>,
}
/// ProduceResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 0, valid_versions = "3-13", flexible_versions = "9+")]
pub struct ProduceResponse {
    #[kafka(versions = "0+")]
    pub responses: Vec<ProduceResponseTopicProduceResponse>,
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "10+")]
    pub node_endpoints: Vec<ProduceResponseNodeEndpoint>,
}


/// ProduceResponseTopicProduceResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceResponseTopicProduceResponse {
    #[kafka(versions = "0-12")]
    pub name: String,
    #[kafka(versions = "13+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partition_responses: Vec<ProduceResponsePartitionProduceResponse>,
}

/// ProduceResponsePartitionProduceResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceResponsePartitionProduceResponse {
    #[kafka(versions = "0+")]
    pub index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub base_offset: i64,
    #[kafka(versions = "2+")]
    pub log_append_time_ms: i64,
    #[kafka(versions = "5+")]
    pub log_start_offset: i64,
    #[kafka(versions = "8+")]
    pub record_errors: Vec<ProduceResponseBatchIndexAndErrorMessage>,
    #[kafka(versions = "8+")]
    pub error_message: String,
    #[kafka(versions = "10+")]
    pub current_leader: ProduceResponseLeaderIdAndEpoch,
}

/// ProduceResponseBatchIndexAndErrorMessage
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceResponseBatchIndexAndErrorMessage {
    #[kafka(versions = "8+")]
    pub batch_index: i32,
    #[kafka(versions = "8+")]
    pub batch_index_error_message: String,
}

/// ProduceResponseLeaderIdAndEpoch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceResponseLeaderIdAndEpoch {
    #[kafka(versions = "10+")]
    pub leader_id: i32,
    #[kafka(versions = "10+")]
    pub leader_epoch: i32,
}

/// ProduceResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ProduceResponseNodeEndpoint {
    #[kafka(versions = "10+")]
    pub node_id: i32,
    #[kafka(versions = "10+")]
    pub host: String,
    #[kafka(versions = "10+")]
    pub port: i32,
    #[kafka(versions = "10+")]
    pub rack: String,
}
