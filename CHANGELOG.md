# Changelog

## [0.8.0] - 2026-08-09

> Additive release: no existing API changed or was removed. The version bump to
> `0.8.0` reflects the new public dependency commitment noted below.
>
> **The raw-frame APIs added here are experimental** (`build_framed`,
> `build_sequential`, `send_raw_frame`, `pub mod wire`). They are expected to
> evolve as proxy/relay use cases become clearer, and may change in a future
> minor release. The stable `Client` / `Producer` / `Consumer` APIs are
> unaffected.
>
> **One caveat for downstream crates defining custom messages.** The
> `#[derive(KafkaMessage)]` macro expands paths rooted at
> `kafka_client_protocol_core` (and `bytes`), so a crate that derives its own
> `Request`/`Response` types must list `kafka-client-protocol-core` in its
> `[dependencies]`. This is the standard serde-style arrangement. Crates that
> only *use* the built-in message types (via `kafka_client::protocol`) need no
> extra dependencies. See the "Custom `Request`/`Response` types" entry below.

### Added

- **Layered public API for raw frame access.** All four layers of the crate are
  now public, so you can drop down exactly as far as a use case requires:
  `Client` (L4) → `connection` (L3) → `wire` (L2) → `transport` (L1). See the
  crate-level docs for the layering diagram and escape hatches.
- **`connection::Builder::build_framed()`** — returns
  `(KafkaFramed, NegotiatedVersions)`: an authenticated, length-prefixed frame
  stream with no reactor. Intended for proxy/gateway use cases doing 1:1 frame
  relay. Because there is no reactor, downstream correlation IDs pass through
  untouched. The stream can be `.split()` into independent read/write halves
  for full-duplex forwarding. See `examples/framed_relay.rs`.
- **`connection::Builder::build_sequential()`** — authenticated connection
  stopped at the one-request-at-a-time stage, before a reactor is spawned.
  Upgrade later via `SequentialConnection::into_pipeline()`.
- **`connection::ConnectionHandle::send_raw_frame(Bytes) -> Result<Bytes>`** —
  send a pre-encoded request frame over an existing pooled connection and get
  the undecoded response back. Note that the caller owns correlation-ID
  uniqueness; prefer `build_framed()` when relaying IDs you do not control.
- **`wire::KafkaFramed`** — a crate-owned wrapper around
  `tokio_util::codec::Framed<NetworkStream, KafkaCodec>`, so the public type
  stays stable even if the underlying codec evolves. Provides three request
  helpers spanning fully-typed to fully-raw:
  - `send_request(Req, api_version, client_id) -> Resp` — fully typed; the
    library encodes the header, body and correlation ID via the `Request`/
    `Response` traits.
  - `send_frame(api_key, api_version, is_flexible, client_id, body) -> Bytes` —
    you supply the API key/version and a pre-encoded body; the library encodes
    the header (picking v1/v2 from `is_flexible`) and owns the correlation ID.
    Sits between `send_request` and `send_raw_frame`.
  - `send_raw_frame(Bytes) -> Result<Bytes>` — fully pre-encoded: `data` is
    written verbatim (only the length prefix is added) and the raw response is
    returned unchanged. The caller owns correlation-ID uniqueness.
  - `recv_response() -> (i32, Bytes)` — read the next response frame with its
    header stripped.
  - `into_inner() -> Framed<...>` — the escape hatch that reaches the raw
    `Framed` for `.split()`-based full-duplex relaying.
  `build_framed()` and `SequentialConnection::into_framed()` return this type.
- **`pub mod wire`** exposing `KafkaCodec`, `KafkaFrame`, `KafkaFramed` and the
  new `DEFAULT_MAX_FRAME_SIZE` constant and `KafkaCodec::max_frame_size()`
  accessor.
- **`connection::Builder::with_max_frame_size()`** — the codec's 100 MiB limit
  is now configurable, for relaying large record batches.
- `tokio_util` is re-exported at the crate root so downstream crates can name
  the `Framed` types produced by `KafkaFramed::into_inner()` without declaring
  their own dependency.
- **Custom `Request`/`Response` types for `send_request`.** A downstream crate
  can now define its own messages via `#[derive(KafkaMessage)]` (and implement
  the public `Request`/`Response` traits manually if needed); the derived types
  satisfy the exact `Req: Request` / `Resp: Response` bounds that
  `send_request` requires, so a user-authored request can be sent with the same
  typed path as a built-in one.
  - `#[derive(KafkaMessage)]` expands to paths rooted at
    `kafka_client_protocol_core` (and `bytes`), both of which the downstream
    crate must list in its `[dependencies]`. `kafka-client-protocol` re-exports
    the derive and the `Request`/`Response`/`Message` traits from core, but the
    generated code references core by its crate name directly, so core must be a
    direct dependency of any crate that *derives* custom messages (this mirrors
    the well-known `serde`/`serde_derive` arrangement).

### Fixed

- **`ConnectionHandle` raw sends no longer hang forever on a lost response.**
  The new `send_raw_frame` applies the same `request_timeout` as
  `send_request`, rather than awaiting the oneshot channel unbounded.

### Internal

- `Builder::build()` now routes through a single private `establish()` pipeline
  shared by `build_sequential()` and `build_framed()`, so every entry point
  performs an identical TCP → TLS → ApiVersions → SASL sequence.
- `Builder` now uses `transport::TransportConnector::connect()` instead of
  hand-rolling TCP/TLS setup, while preserving the documented
  `KafkaError::Io` vs `KafkaError::TlsError` distinction.
- The reactor routes pending responses by correlation ID using a named,
  tested `request_correlation_id()` helper, documenting why request headers read
  bytes `[4..8]` while response headers read `[0..4]`. The `wire` layer's
  `send_raw_frame` performs no such extraction — it forwards pre-encoded bytes
  untouched.

### Note on public dependencies

`build_framed()` returns `wire::KafkaFramed`, a crate-owned wrapper that holds
a `tokio_util::codec::Framed` internally. The stable surface (`send_request`,
`recv_response`, `send_raw_frame`) does not name `tokio-util`, but the
`into_inner()` escape hatch returns a raw `Framed`, so `tokio-util 0.7` is still
part of this crate's public API. A future `tokio-util 0.8` will therefore be a
breaking change for `kafka_client`. This is a deliberate trade-off: reaching
the raw `Framed` is what allows `.split()` and `Sink`/`Stream` composition,
which proxy use cases require, while the wrapper itself keeps our main type name
stable.

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
