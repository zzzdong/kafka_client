//! Auto-generated from Kafka protocol
//! Message: UpdateRaftVoterRequest
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
pub struct KRaftVersionFeature {
    /// The minimum supported KRaft protocol version.
    #[kafka(versions = "0+")]
    pub min_supported_version: i16,
    /// The maximum supported KRaft protocol version.
    #[kafka(versions = "0+")]
    pub max_supported_version: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 82,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct UpdateRaftVoterRequest {
    /// The cluster id.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub cluster_id: Option<String>,
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    #[kafka(versions = "0+")]
    pub current_leader_epoch: i32,
    /// The replica id of the voter getting updated in the topic partition.
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    /// The directory id of the voter getting updated in the topic partition.
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
    /// The endpoint that can be used to communicate with the leader.
    #[kafka(versions = "0+")]
    pub listeners: Vec<Listener>,
    /// The range of versions of the protocol that the replica supports.
    #[kafka(versions = "0+")]
    pub kraft_version_feature: KRaftVersionFeature,
}
