//! Metadata API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 3

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// MetadataRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 3, valid_versions = "0-13", flexible_versions = "9+")]
pub struct MetadataRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<MetadataRequestMetadataRequestTopic>,
    #[kafka(versions = "4+")]
    pub allow_auto_topic_creation: bool,
    #[kafka(versions = "8-10")]
    pub include_cluster_authorized_operations: bool,
    #[kafka(versions = "8+")]
    pub include_topic_authorized_operations: bool,
}


/// MetadataRequestMetadataRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct MetadataRequestMetadataRequestTopic {
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub name: String,
}
/// MetadataResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 3, valid_versions = "0-13", flexible_versions = "9+")]
pub struct MetadataResponse {
    #[kafka(versions = "3+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub brokers: Vec<MetadataResponseMetadataResponseBroker>,
    #[kafka(versions = "2+")]
    pub cluster_id: String,
    #[kafka(versions = "1+")]
    pub controller_id: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<MetadataResponseMetadataResponseTopic>,
    #[kafka(versions = "8-10")]
    pub cluster_authorized_operations: i32,
    #[kafka(versions = "13+")]
    pub error_code: i16,
}


/// MetadataResponseMetadataResponseBroker
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct MetadataResponseMetadataResponseBroker {
    #[kafka(versions = "0+")]
    pub node_id: i32,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i32,
    #[kafka(versions = "1+")]
    pub rack: String,
}

/// MetadataResponseMetadataResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct MetadataResponseMetadataResponseTopic {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    #[kafka(versions = "1+")]
    pub is_internal: bool,
    #[kafka(versions = "0+")]
    pub partitions: Vec<MetadataResponseMetadataResponsePartition>,
    #[kafka(versions = "8+")]
    pub topic_authorized_operations: i32,
}

/// MetadataResponseMetadataResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct MetadataResponseMetadataResponsePartition {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "7+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub replica_nodes: Vec<i32>,
    #[kafka(versions = "0+")]
    pub isr_nodes: Vec<i32>,
    #[kafka(versions = "5+")]
    pub offline_replicas: Vec<i32>,
}
