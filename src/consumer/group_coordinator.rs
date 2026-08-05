//! GroupCoordinator — independent actor for consumer group coordination
//!
//! Responsibilities:
//! - Join/Sync group protocol
//! - Background heartbeat loop
//! - Sends GroupEvent to the orchestrator on assignment/lifecycle changes

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, warn};

use crate::cluster::ClusterClient;
use crate::consumer::config::{ConsumerConfig, PartitionAssignmentStrategy};
use crate::consumer::util::{
    build_protocol_metadata, compute_all_assignments, find_coordinator_raw, map_heartbeat_error,
};
use crate::error::{KafkaError, Result};
use crate::protocol::{
    ConsumerProtocolAssignment, HeartbeatRequest, JoinGroupRequest, JoinGroupRequestProtocol,
    JoinGroupResponse, SyncGroupRequest, SyncGroupRequestAssignment, SyncGroupResponse,
};
use bytes::{Bytes, BytesMut};
use kafka_client_protocol::{KafkaErrorCode, Message};

// ===========================================================================
// Communication types
// ===========================================================================

pub(crate) enum GroupCommand {
    /// Join the group with the given topics (starts join+sync protocol).
    Join {
        topics: Vec<String>,
        /// The member's current assignment, carried to the leader so sticky
        /// balancing can minimize partition movement.
        previous_assignment: HashMap<String, Vec<i32>>,
    },
    /// Leave the group (stop heartbeat, orchestrator handles the Leave RPC).
    Leave,
    /// Shut down the coordinator task.
    Shutdown,
}

#[derive(Debug)]
pub(crate) enum GroupEvent {
    /// Join+Sync completed; the consumer has been assigned partitions.
    AssignmentChanged {
        partitions: HashMap<String, Vec<i32>>,
        coordinator: SocketAddr,
        member_id: String,
        generation_id: i32,
    },
    /// Heartbeat returned REBALANCE_IN_PROGRESS — need to rejoin.
    RebalanceRequired,
    /// Non-recoverable heartbeat error.
    FatalError(KafkaError),
}

pub(crate) struct GroupCoordinatorHandle {
    pub cmd_tx: mpsc::UnboundedSender<GroupCommand>,
    #[allow(dead_code)]
    pub join_handle: tokio::task::JoinHandle<()>,
}

impl GroupCoordinatorHandle {
    pub async fn shutdown(self) {
        let _ = self.cmd_tx.send(GroupCommand::Shutdown);
    }
}

// ===========================================================================
// GroupCoordinator actor
// ===========================================================================

pub(crate) fn spawn_group_coordinator(
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
) -> (GroupCoordinatorHandle, mpsc::UnboundedReceiver<GroupEvent>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let join_handle = tokio::spawn(async move {
        let mut gc = GroupCoordinator {
            cluster,
            config,
            cmd_rx,
            event_tx,
            state: GroupState::Idle,
            member_id: String::new(),
            generation_id: 0,
            leader: String::new(),
            protocol_name: None,
            coordinator: None,
            subscribed_topics: Vec::new(),
            previous_assignment: HashMap::new(),
        };
        gc.run().await;
    });

    let handle = GroupCoordinatorHandle {
        cmd_tx,
        join_handle,
    };
    (handle, event_rx)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GroupState {
    Idle,
    Joining,
    Stable,
}

struct GroupCoordinator {
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    cmd_rx: mpsc::UnboundedReceiver<GroupCommand>,
    event_tx: mpsc::UnboundedSender<GroupEvent>,

    state: GroupState,
    member_id: String,
    generation_id: i32,
    leader: String,
    protocol_name: Option<String>,
    coordinator: Option<SocketAddr>,
    subscribed_topics: Vec<String>,
    previous_assignment: HashMap<String, Vec<i32>>,
}

impl GroupCoordinator {
    async fn run(&mut self) {
        let mut heartbeat_interval = interval(self.config.heartbeat_interval);
        heartbeat_interval.reset(); // don't tick immediately

        loop {
            match self.state.clone() {
                GroupState::Idle | GroupState::Joining => {
                    tokio::select! {
                        cmd = self.cmd_rx.recv() => {
                            match cmd {
                                Some(GroupCommand::Join {
                                    topics,
                                    previous_assignment,
                                }) => {
                                    self.subscribed_topics = topics;
                                    self.previous_assignment = previous_assignment;
                                    self.join_with_retry().await;
                                }
                                Some(GroupCommand::Leave) => {
                                    self.reset_state();
                                }
                                Some(GroupCommand::Shutdown) | None => break,
                            }
                        }
                    }
                }
                GroupState::Stable => {
                    tokio::select! {
                        _ = heartbeat_interval.tick() => {
                            if !self.handle_heartbeat_tick().await {
                                break; // orchestrator dropped
                            }
                        }
                        cmd = self.cmd_rx.recv() => {
                            match cmd {
                                Some(GroupCommand::Join {
                                    topics,
                                    previous_assignment,
                                }) => {
                                    self.subscribed_topics = topics;
                                    self.previous_assignment = previous_assignment;
                                    self.join_with_retry().await;
                                }
                                Some(GroupCommand::Leave) => {
                                    self.reset_state();
                                }
                                Some(GroupCommand::Shutdown) | None => break,
                            }
                        }
                    }
                }
            }
        }
        debug!("GroupCoordinator shutting down");
    }

    /// Retry `do_join()` on failure with exponential backoff + jitter.
    ///
    /// - Initial backoff: 200ms
    /// - Doubles each attempt, capped at `session_timeout / 2`
    /// - Random jitter of ±50% to avoid thundering herd
    ///
    /// The loop is interruptible by `Leave` and `Shutdown` commands.
    async fn join_with_retry(&mut self) {
        const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
        let max_backoff = {
            let raw = self.config.session_timeout / 2;
            // cap at 5s so even a very long session timeout doesn't make
            // the first-rebalance delay unbearable
            raw.min(Duration::from_secs(5))
        };
        let mut backoff = INITIAL_BACKOFF;

        loop {
            self.state = GroupState::Joining;
            self.do_join().await;
            if self.state == GroupState::Stable {
                // Send an immediate heartbeat to establish the session,
                // rather than waiting for the next interval tick.
                self.handle_heartbeat_tick().await;
                return; // success
            }

            // Join failed — sleep with backoff, listen for interrupts
            // Apply ±50% jitter so concurrent consumers don't retry in lockstep
            let jitter = {
                let half = backoff.as_millis() as u64 / 2;
                let offset = if half > 0 {
                    rand::random::<u64>() % (half * 2 + 1)
                } else {
                    0
                };
                Duration::from_millis((half + offset).max(1))
            };

            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(GroupCommand::Leave) => {
                            self.reset_state();
                            return;
                        }
                        Some(GroupCommand::Shutdown) | None => return,
                        _ => {} // other commands: ignore, continue retry
                    }
                }
                _ = tokio::time::sleep(jitter) => {
                    // Backoff elapsed — retry join
                }
            }

            // Exponential backoff, capped
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    // ------------------------------------------------------------------
    // Join+Sync protocol
    // ------------------------------------------------------------------

    async fn do_join(&mut self) {
        let group_id = match self.config.group_id.as_deref() {
            Some(id) => id.to_string(),
            None => return,
        };

        let coord_addr = match find_coordinator_raw(&self.cluster, &group_id).await {
            Ok(addr) => {
                self.coordinator = Some(addr);
                addr
            }
            Err(e) => {
                warn!("find_coordinator failed: {:?}, will retry", e);
                self.reset_state();
                return;
            }
        };

        let protocol_metadata =
            build_protocol_metadata(&self.subscribed_topics, &self.previous_assignment);

        let mut retry_member_id = std::mem::take(&mut self.member_id);
        for _ in 0..10 {
            let name = match self.config.partition_assignment_strategy {
                PartitionAssignmentStrategy::Range => "range",
                PartitionAssignmentStrategy::RoundRobin => "roundrobin",
                PartitionAssignmentStrategy::Sticky => "sticky",
                PartitionAssignmentStrategy::CooperativeSticky => "cooperative-sticky",
            }
            .to_string();

            let request = JoinGroupRequest {
                group_id: group_id.clone(),
                session_timeout_ms: self.config.session_timeout.as_millis() as i32,
                rebalance_timeout_ms: self.config.rebalance_timeout.as_millis() as i32,
                member_id: retry_member_id.clone(),
                group_instance_id: None,
                protocol_type: "consumer".to_string(),
                protocols: vec![JoinGroupRequestProtocol {
                    name,
                    metadata: protocol_metadata.clone(),
                }],
                reason: None,
            };

            let response: JoinGroupResponse =
                match self.cluster.send_to_broker(coord_addr, &request).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("JoinGroup request failed: {:?}", e);
                        continue;
                    }
                };

            if response.error_code == 0 {
                self.member_id = response.member_id.clone();
                self.generation_id = response.generation_id;
                self.leader = response.leader.clone();
                self.protocol_name = response.protocol_name.clone();

                // SyncGroup
                match self.do_sync(&group_id, &response).await {
                    Ok(partitions) => {
                        if let Some(coord) = self.coordinator
                            && self
                                .event_tx
                                .send(GroupEvent::AssignmentChanged {
                                    partitions,
                                    coordinator: coord,
                                    member_id: self.member_id.clone(),
                                    generation_id: self.generation_id,
                                })
                                .is_err()
                        {
                            debug!("Orchestrator dropped, shutting down GroupCoordinator");
                            self.reset_state();
                            return;
                        }
                        debug!(
                            "Joined group, member_id={}, generation_id={}",
                            self.member_id, self.generation_id
                        );
                        self.state = GroupState::Stable;
                        return;
                    }
                    Err(e) => {
                        warn!("SyncGroup failed: {:?}", e);
                        self.reset_state();
                        return;
                    }
                }
            }

            if response.error_code == KafkaErrorCode::MEMBER_ID_REQUIRED.code() {
                if response.member_id.is_empty() || response.member_id == retry_member_id {
                    warn!("MEMBER_ID_REQUIRED with no new member_id");
                    self.reset_state();
                    return;
                }
                warn!(
                    "MEMBER_ID_REQUIRED, retrying with member_id={}",
                    response.member_id
                );
                retry_member_id = response.member_id.clone();
                // Yield to avoid hammering the broker with back-to-back requests.
                tokio::task::yield_now().await;
                continue;
            }

            warn!("JoinGroup error code={}", response.error_code);
            self.reset_state();
            return;
        }

        warn!("JoinGroup retry exhausted");
        self.reset_state();
    }

    async fn do_sync(
        &self,
        group_id: &str,
        join_response: &JoinGroupResponse,
    ) -> Result<HashMap<String, Vec<i32>>> {
        let coord = self.coordinator.ok_or(KafkaError::NoCoordinator)?;

        let all_assignments = compute_all_assignments(
            &self.subscribed_topics,
            join_response,
            &self.cluster,
            self.config.partition_assignment_strategy,
        )
        .await?;

        let sync_assignments: Vec<SyncGroupRequestAssignment> = join_response
            .members
            .iter()
            .map(|m| {
                let assignment_bytes = all_assignments
                    .get(&m.member_id)
                    .map(|a| {
                        let mut buf = BytesMut::new();
                        a.encode(&mut buf, 0)
                            .map_err(|e| KafkaError::Protocol(e.to_string()))?;
                        Ok::<_, KafkaError>(buf.freeze())
                    })
                    .unwrap_or(Ok(Bytes::new()))?;
                Ok(SyncGroupRequestAssignment {
                    member_id: m.member_id.clone(),
                    assignment: assignment_bytes,
                })
            })
            .collect::<Result<_>>()?;

        let sync_request = SyncGroupRequest {
            group_id: group_id.to_string(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: self.protocol_name.clone(),
            assignments: sync_assignments,
        };
        let sync_response: SyncGroupResponse =
            self.cluster.send_to_broker(coord, &sync_request).await?;

        if sync_response.error_code != 0 {
            return Err(KafkaError::Protocol(format!(
                "SyncGroup error: {}",
                sync_response.error_code
            )));
        }

        let mut buf_data = sync_response.assignment;
        let assignment_result = ConsumerProtocolAssignment::decode(&mut buf_data, 0)
            .map_err(|e| KafkaError::Protocol(e.to_string()))?;
        let mut partitions: HashMap<String, Vec<i32>> = HashMap::new();
        for tp in assignment_result.assigned_partitions {
            partitions
                .entry(tp.topic)
                .or_default()
                .extend(tp.partitions);
        }
        Ok(partitions)
    }

    // ------------------------------------------------------------------
    // Heartbeat
    // ------------------------------------------------------------------

    /// Returns `false` if the orchestrator receiver is gone (caller should shut down).
    async fn handle_heartbeat_tick(&mut self) -> bool {
        let Some(coord) = self.coordinator else {
            return true;
        };
        let Some(group_id) = self.config.group_id.as_deref() else {
            return true;
        };

        let request = HeartbeatRequest {
            group_id: group_id.to_string(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
        };
        let response: Result<crate::protocol::HeartbeatResponse> =
            self.cluster.send_to_broker(coord, &request).await;

        match response {
            Ok(r) => {
                if let Err(e) =
                    map_heartbeat_error(r.error_code, self.generation_id, &self.member_id)
                {
                    match &e {
                        KafkaError::RebalanceRequired => {
                            if self.event_tx.send(GroupEvent::RebalanceRequired).is_err() {
                                debug!("Orchestrator dropped, shutting down GroupCoordinator");
                                return false;
                            }
                            self.reset_state();
                        }
                        KafkaError::IllegalGeneration(_) | KafkaError::UnknownMemberId(_) => {
                            if self.event_tx.send(GroupEvent::FatalError(e)).is_err() {
                                debug!("Orchestrator dropped, shutting down GroupCoordinator");
                                return false;
                            }
                            self.reset_state();
                        }
                        _ => debug!("Heartbeat failed (non-fatal): {}", e),
                    }
                }
            }
            Err(e) => debug!("Heartbeat request failed: {}", e),
        }
        true
    }

    fn reset_state(&mut self) {
        self.member_id.clear();
        self.generation_id = 0;
        self.leader.clear();
        self.protocol_name = None;
        self.coordinator = None;
        self.previous_assignment.clear();
        self.state = GroupState::Idle;
    }
}
