# Changelog

## [0.7.0] - 2026-08-07

> **Version note:** `kafka_client` bumped to `0.7.0` and `krb5-gss` bumped to `0.2.0`
> because this release removes the public `with_strict_aprep` APIs (a breaking change).

### Breaking

- **`krb5-gss`: GSSAPI mutual authentication is now always strict.**
  Removed `GssClient::with_strict_aprep` and `NativeGssContext::with_strict_aprep`
  (and the `strict` parameter of the internal `verify_ap_rep_token`). GSSAPI mutual
  auth is protocol-mandated (the client sends `GSS_C_MUTUAL_FLAG`), so a missing or
  unverifiable AP-REP now always aborts the handshake instead of degrading to a
  warning. This closes the intermediate-attacker path where a non-verifying acceptor
  could be accepted. If you relied on non-strict mode to talk to a non-standard
  acceptor, you must ensure it returns a valid AP-REP.

### Fixed

- **`krb5-gss`: correctly parse RFC 4120 `EncAPRepPart ::= [APPLICATION 27] SEQUENCE`**
  instead of skipping a hard-coded 2-byte offset. The AP-REP enc-part plaintext from
  Java Kafka brokers (SunJGSS) begins with `[APPLICATION 27]` (tag `0x7b`); the decoder
  now genuinely unwraps that application tag rather than blindly jumping 2 bytes.

## [0.6.1] - 2026-08-05

> `kafka-client-protocol` and `kafka-client-protocol-core` are bumped to
> 0.2.2 for this release (adds `RecordBatch::is_control_batch`).

### Fixed

- `AdminClient::create_topics` with `with_replica_assignments` now assigns
  real partition indexes (`0..N-1`) instead of `-1`, making manual replica
  assignments work.
- `send_to_any_broker` no longer aborts on the first broker that fails to
  connect; it records the error and keeps trying the remaining brokers.
- Reconnects (unhealthy broker, force-close, new connection) now reuse the
  broker hostname advertised in metadata, keeping the Kerberos service
  principal (`krb/hostname`) stable across reconnects.
- Brokers that disappear from the metadata response are pruned from the
  broker pool instead of accumulating stale entries forever.
- Every broker request now has a timeout (default 60s, configurable via
  `ClientBuilder::with_request_timeout`); requests that receive no response
  return `KafkaError::RequestTimeout` instead of hanging forever.
- A response frame shorter than 4 bytes no longer panics the connection
  reactor; the connection is closed with a protocol error instead.
- `AdminClient::list_groups` queries brokers in parallel and logs a warning
  when the result is incomplete due to broker failures.
- `AdminClient::describe_groups` resolves coordinators and describes groups
  in parallel instead of one serial round-trip per group.
- `AdminClient::fetch_group_offsets` propagates the broker-reported group
  error code via `KafkaError::GroupError` instead of collapsing it to
  `NoCoordinator`.
- Group-mode `Consumer::subscribe` no longer blocks forever when the
  coordinator never assigns partitions; it times out after
  `rebalance_timeout + session_timeout` and returns `RequestTimeout` while
  the join keeps retrying in the background.
- Consumer now skips control batches (transaction abort/commit markers)
  instead of surfacing them as empty records; the fetch position still
  advances past the markers.
- Consumer group rebalancing:
  - New `PartitionAssignmentStrategy::Sticky` — a real sticky assignor that
    balances partitions while minimizing movement; each member carries its
    previous assignment in the subscription `user_data` so the leader can
    preserve ownership across rebalances.
  - `CooperativeSticky` now uses the same sticky balancing (still on the
    classic JoinGroup/SyncGroup protocol; the incremental KIP-429 phases are
    not implemented).
  - Rebalance race handling: fetch results for partitions reassigned away
    are ignored, offset commits only cover currently assigned partitions,
    and stale cursors/offsets are pruned when a new assignment arrives.
- Idempotent producer: a failed batch rolls the partition sequence number
  back so subsequent sends reuse it (deduplicated by the broker), instead of
  leaving a permanent sequence gap.
- Transactional producer: after an *aborted* transaction the per-partition
  sequence numbers are restored to the transaction-start snapshot, mirroring
  the broker's rollback; previously the next transaction failed with
  `OUT_OF_ORDER_SEQUENCE_NUMBER`. `AddPartitionsToTxn` also recovers from
  fenced producer errors by re-initializing the producer id (epoch bump),
  and `EndTxn` adopts the producer id/epoch returned by v5+ brokers
  (KIP-890).
- `MESSAGE_TOO_LARGE` batches are now split repeatedly (not just once) until
  each record is sent individually.
- SASL PLAIN now sends the authorisation identity (`authzid`, settable via
  `SaslCredentials::with_authzid`) per RFC 4616 instead of always sending an
  empty one.
- Kerberos GSSAPI handshakes now verify the broker's AP-REP (mutual
  authentication) against real Kafka brokers: the SunJGSS enc-part layout
  (2-byte `0x7b 0x24` prefix before the `EncAPRepPart` DER) is accepted, and
  both key usages 12 (SunJGSS) and 15 (RFC 4120) are tried. Strict AP-REP
  verification is enabled on the Kafka connection path.
- Kerberos multi-broker: each broker connection now resolves and remembers
  its own advertised hostname and authenticates with its own
  `kafka/<host>` service principal, instead of a global
  `with_broker_hostname` value baked into the shared credentials (which
  allowed only one broker in a multi-broker cluster to authenticate).
- Hash-key partitioning now matches Java clients: `(murmur2 & 0x7fffffff) %
  partitions` instead of `abs(murmur2) % partitions`, so mixed Java/Rust
  producers agree on partition placement.

### Added

- `ClientBuilder::with_request_timeout` / `ClientConfig::request_timeout`.
- `SaslCredentials::with_authzid`.
- `AdminGroup::state` is now populated from `ListGroupsResponse` when the
  broker reports it.
- **Idempotent producer is now enabled by default** (`acks=-1`, effectively
  unbounded retries bounded by the delivery timeout), like modern Kafka
  clients. Disable with `ProducerConfig::with_idempotence(false)` when
  `acks=0/1` or pre-0.11 brokers are needed.
- **Transactional producer (Kafka EOS / KIP-98)**: `ProducerConfig::
  with_transactional_id` plus `Producer::init_transactions`,
  `begin_transaction`, `commit_transaction`, `abort_transaction`, and
  `send_offsets_to_transaction` (TxnOffsetCommit, the consume-process-produce
  bridge). The client lazily registers partitions with the transaction
  coordinator (`AddPartitionsToTxn`), produces with the transactional id and
  PID/epoch, and recovers from fatal transaction errors by re-initializing
  the producer id (epoch bump).
- `Consumer::group().generation()` / `member_id()` expose the group
  generation and member id needed for transactional offset commits.
- New errors: `KafkaError::TransactionError`, `KafkaError::
  InvalidTransactionState`.
- Admin coverage:
  - ACLs: `create_acls` / `describe_acls` / `delete_acls` with typed
    `AclBinding` / `AclBindingFilter` (`AclResourceType`, `AclOperation`,
    `AclPermissionType`, `AclPatternType`).
  - Configuration: `alter_configs` / `alter_topic_configs` /
    `describe_configs`; `KafkaError::AdminError` for broker-reported errors.
  - `delete_records` (routed to partition leaders) and
    `reset_group_offsets`.
- `AdminClient::create_topics` / `delete_topics` are now routed to the
  controller and retry (with a metadata refresh) when the broker replies
  `NOT_CONTROLLER`, instead of failing on a random non-controller broker.
- `AdminClient::commit_offsets` refreshes metadata when the topic id is
  missing from the cache and retries on `UNKNOWN_TOPIC_ID` (a just-created
  topic may briefly lag the local metadata); the ACL integration test retries
  `describe_acls` to absorb KRaft ACL propagation.
- Integration tests for transactions (`tests/transactions.rs`); the
  `producer_acks` tests now explicitly disable idempotence for `acks=0/1`.
- **Expanded test coverage**:
  - Unit tests: wire frame codec (framing, negative/oversized lengths),
    producer config defaults and record batch building, consumer config
    defaults, negotiated versions, transport security protocol flags,
    `ClientConfig` defaults, error display.
  - Integration tests: `tests/admin.rs` (topic/cluster/broker-config admin,
    group lifecycle with commit/fetch/delete), `tests/producer_api.rs`
    (`send_batch`/`send_direct`/`flush`/`close`), `tests/consumer_api.rs`
    (direct assign, seek, `max_poll_records`, `try_poll`/`poll_timeout`).
    The ACL test lives in its own `tests/acl.rs` against a dedicated
    authorizer-enabled cluster (`tests/docker-compose.acl.yml`, port 9098)
    and skips gracefully on clusters without an authorizer
    (`SECURITY_DISABLED`); `tests/run-all-tests.sh` starts/stops it.
  - `tests/run-all-tests.sh` now includes `admin`, `producer_api`,
    `consumer_api` and `transactions`.
  - New Kerberos multi-broker integration test (`tests/kerberos_multi.rs` +
    `tests/docker-compose.kerberos-multi.yml`): three brokers each advertise
    a distinct hostname with its own `kafka/<host>` service principal; the
    client intentionally sets a global `with_broker_hostname` and must still
    authenticate to all brokers via their per-broker advertised host. The
    test binary is statically compiled on the host (musl target) and run
    inside a compose `test-runner` container on the cluster network, so the
    broker hostnames resolve via container DNS without touching `/etc/hosts`.
