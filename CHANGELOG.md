# Changelog

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
- Integration tests for transactions (`tests/transactions.rs`); the
  `producer_acks` tests now explicitly disable idempotence for `acks=0/1`.
