//! AlterPartition API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 56

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// AlterPartitionRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 56, valid_versions = "2-3", flexible_versions = "0+")]
pub struct AlterPartitionRequest {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    #[kafka(versions = "0+")]
    pub broker_epoch: i64,
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterPartitionRequestTopicData>,
}


/// AlterPartitionRequestTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionRequestTopicData {
    #[kafka(versions = "2+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterPartitionRequestPartitionData>,
}

/// AlterPartitionRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0-2")]
    pub new_isr: Vec<i32>,
    #[kafka(versions = "3+")]
    pub new_isr_with_epochs: Vec<AlterPartitionRequestBrokerState>,
    #[kafka(versions = "1+")]
    pub leader_recovery_state: i8,
    #[kafka(versions = "0+")]
    pub partition_epoch: i32,
}

/// AlterPartitionRequestBrokerState
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionRequestBrokerState {
    #[kafka(versions = "3+")]
    pub broker_id: i32,
    #[kafka(versions = "3+")]
    pub broker_epoch: i64,
}
/// AlterPartitionResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 56, valid_versions = "2-3", flexible_versions = "0+")]
pub struct AlterPartitionResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterPartitionResponseTopicData>,
}


/// AlterPartitionResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionResponseTopicData {
    #[kafka(versions = "2+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterPartitionResponsePartitionData>,
}

/// AlterPartitionResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterPartitionResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub isr: Vec<i32>,
    #[kafka(versions = "1+")]
    pub leader_recovery_state: i8,
    #[kafka(versions = "0+")]
    pub partition_epoch: i32,
}
