//! Auto-generated from Kafka protocol
//! Message: DescribeClusterResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribeClusterBroker {
    /// The broker ID.
    #[kafka(versions = "0+", map_key)]
    pub broker_id: i32,
    /// The broker hostname.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The broker port.
    #[kafka(versions = "0+")]
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub rack: Option<String>,
    /// Whether the broker is fenced
    #[kafka(versions = "2+")]
    pub is_fenced: bool,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 60, msg_type = "response", valid_versions = "0-2", flexible_versions = "0+")]
pub struct DescribeClusterResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top-level error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The endpoint type that was described. 1=brokers, 2=controllers.
    #[kafka(versions = "1+", default = 1)]
    pub endpoint_type: i8,
    /// The cluster ID that responding broker belongs to.
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    /// The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
    #[kafka(versions = "0+", default = -1)]
    pub controller_id: i32,
    /// Each broker in the response.
    #[kafka(versions = "0+")]
    pub brokers: Vec<DescribeClusterBroker>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    #[kafka(versions = "0+", default = -2147483648)]
    pub cluster_authorized_operations: i32,
}

