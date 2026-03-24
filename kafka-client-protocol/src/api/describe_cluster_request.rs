//! Auto-generated from Kafka protocol
//! Message: DescribeClusterRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 60, msg_type = "request", valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeClusterRequest {
    /// Whether to include cluster authorized operations.
    #[kafka(versions = "0+")]
    pub include_cluster_authorized_operations: bool,
    /// The endpoint type to describe. 1=brokers, 2=controllers.
    #[kafka(versions = "1+", default = 1)]
    pub endpoint_type: i8,
    /// Whether to include fenced brokers when listing brokers.
    #[kafka(versions = "2+")]
    pub include_fenced_brokers: bool,
}

