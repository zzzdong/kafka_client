//! Auto-generated from Kafka protocol
//! Message: VotersRecord
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Endpoint {
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
pub struct Voter {
    /// The replica id of the voter in the topic partition.
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    /// The directory id of the voter in the topic partition.
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
    /// The endpoint that can be used to communicate with the voter.
    #[kafka(versions = "0+")]
    pub endpoints: Vec<Endpoint>,
    /// The range of versions of the protocol that the replica supports.
    #[kafka(versions = "0+")]
    pub kraft_version_feature: KRaftVersionFeature,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "0+")]
pub struct VotersRecord {
    /// The version of the voters record.
    #[kafka(versions = "0+")]
    pub version: i16,
    /// The set of voters in the quorum for this epoch.
    #[kafka(versions = "0+")]
    pub voters: Vec<Voter>,
}
