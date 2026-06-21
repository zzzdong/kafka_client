//! Auto-generated from Kafka protocol
//! Message: AddRaftVoterRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The hostname.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The port.
    #[kafka(versions = "0+")]
    pub port: u16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 80,
    msg_type = "request",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct AddRaftVoterRequest {
    /// The cluster id.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub cluster_id: Option<String>,
    /// The maximum time to wait for the request to complete before returning.
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    /// The replica id of the voter getting added to the topic partition.
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    /// The directory id of the voter getting added to the topic partition.
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
    /// The endpoints that can be used to communicate with the voter.
    #[kafka(versions = "0+")]
    pub listeners: Vec<Listener>,
    /// When true, return a response after the new voter set is committed. Otherwise, return after the leader writes the changes locally.
    #[kafka(versions = "1+", default = true)]
    pub ack_when_committed: bool,
}
