//! Auto-generated from Kafka protocol
//! Message: LeaderChangeMessage
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Voter {
    /// The ID of the voter.
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    /// The directory id of the voter.
    #[kafka(versions = "1+")]
    pub voter_directory_id: Uuid,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0-1", flexible_versions = "0+")]
pub struct LeaderChangeMessage {
    /// The version of the leader change message.
    #[kafka(versions = "0+")]
    pub version: i16,
    /// The ID of the newly elected leader.
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    /// The set of voters in the quorum for this epoch.
    #[kafka(versions = "0+")]
    pub voters: Vec<Voter>,
    /// The voters who voted for the leader at the time of election.
    #[kafka(versions = "0+")]
    pub granting_voters: Vec<Voter>,
}

