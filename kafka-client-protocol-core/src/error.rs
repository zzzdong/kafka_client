// src/error.rs
use std::io;
use thiserror::Error;

/// 协议错误类型
///
/// 涵盖 Kafka 协议解析过程中可能出现的所有错误
#[derive(Debug, Error)]
pub enum ProtocolError {
    // ============ IO 相关错误 ============
    /// IO 错误（网络、文件等）
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    // ============ 数据完整性错误 ============
    /// 数据不足，无法完成解码
    #[error("Insufficient data: expected {expected} bytes, got {actual}")]
    InsufficientData { expected: usize, actual: usize },

    /// 数据超出预期大小
    #[error("Data too large: expected max {max}, got {actual}")]
    DataTooLarge { max: usize, actual: usize },

    /// CRC 校验失败
    #[error("CRC checksum failed: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch { expected: u32, actual: u32 },

    /// 无效的数据格式
    #[error("Invalid data format: {0}")]
    InvalidData(String),

    // ============ 版本相关错误 ============
    /// 不支持的 API 版本
    #[error("Unsupported API version: {version} for API key {api_key}")]
    UnsupportedVersion { api_key: i16, version: i16 },

    /// 不支持的 API Key
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),

    /// 版本协商失败
    #[error("Version negotiation failed: {0}")]
    VersionNegotiationFailed(String),

    // ============ 编解码错误 ============
    /// 编码错误
    #[error("Encode error: {0}")]
    Encode(String),

    /// 解码错误
    #[error("Decode error: {0}")]
    Decode(String),

    /// 未知的字段标签
    #[error("Unknown tagged field tag: {0}")]
    UnknownTag(u32),

    // ============ UTF-8 相关错误 ============
    /// UTF-8 解码错误
    #[error("UTF-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    /// 字符串包含 NUL 字符
    #[error("String contains NUL character: {0}")]
    StringContainsNul(String),

    // ============ 其他错误 ============
    /// 其他协议错误
    #[error("Protocol error: {0}")]
    Other(String),
}

/// 协议操作结果类型
pub type ProtocolResult<T> = Result<T, ProtocolError>;

impl ProtocolError {
    /// 创建无效数据错误
    pub fn invalid_data(msg: impl Into<String>) -> Self {
        ProtocolError::InvalidData(msg.into())
    }

    /// 创建数据不足错误
    pub fn insufficient_data(expected: usize, actual: usize) -> Self {
        ProtocolError::InsufficientData { expected, actual }
    }

    /// 创建不支持的版本错误
    pub fn unsupported_version(api_key: i16, version: i16) -> Self {
        ProtocolError::UnsupportedVersion { api_key, version }
    }

    /// 检查错误是否可重试
    pub fn is_retryable(&self) -> bool {
        match self {
            // IO 错误可能可重试
            ProtocolError::Io(e) => {
                // 连接重置、超时等可重试
                e.kind() == io::ErrorKind::ConnectionReset
                    || e.kind() == io::ErrorKind::TimedOut
                    || e.kind() == io::ErrorKind::BrokenPipe
            }
            // 数据不足可能是部分读取，可重试
            ProtocolError::InsufficientData { .. } => true,
            // 其他错误通常不可重试
            _ => false,
        }
    }

    /// 获取错误码（用于 Kafka 协议）
    pub fn error_code(&self) -> Option<KafkaErrorCode> {
        match self {
            ProtocolError::UnsupportedVersion { .. } => Some(KafkaErrorCode::UNSUPPORTED_VERSION),
            ProtocolError::UnsupportedApiKey(_) => Some(KafkaErrorCode::UNSUPPORTED_VERSION),
            ProtocolError::InvalidData(_) => Some(KafkaErrorCode::INVALID_REQUEST),
            ProtocolError::CrcMismatch { .. } => Some(KafkaErrorCode::CORRUPT_MESSAGE),
            _ => None,
        }
    }
}

// ============================================================================
// KafkaErrorCode — Kafka 协议错误码定义
// ============================================================================

/// Kafka 协议错误码，对应 Kafka 协议规范中定义的错误码。
///
/// 每个错误码包含：
/// - 数字编码（`code()`）
/// - 是否可重试（`is_retriable()`）
/// - 人类可读描述（`description()`）
///
/// # 示例
///
/// ```ignore
/// let err = KafkaErrorCode::OFFSET_OUT_OF_RANGE;
/// assert_eq!(err.code(), 1);
/// assert!(!err.is_retriable());
///
/// // 从 broker 响应中的错误码转换
/// let err = KafkaErrorCode::from_i16(response.error_code);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KafkaErrorCode(i16);

impl KafkaErrorCode {
    // =======================================================================
    // 错误码常量定义（按 Kafka 协议规范顺序）
    // =======================================================================

    /// (0) 无错误
    pub const NONE: Self = Self(0);
    /// (1) 请求的偏移量超出服务器维护的范围。不可重试。
    pub const OFFSET_OUT_OF_RANGE: Self = Self(1);
    /// (2) CRC 校验失败、超过有效大小、压缩主题的 null key 或其他损坏。可重试。
    pub const CORRUPT_MESSAGE: Self = Self(2);
    /// (3) 该服务器不托管此主题分区。可重试。
    pub const UNKNOWN_TOPIC_OR_PARTITION: Self = Self(3);
    /// (4) 请求的 fetch 大小无效。不可重试。
    pub const INVALID_FETCH_SIZE: Self = Self(4);
    /// (5) 主题分区的 leader 正在选举中。可重试。
    pub const LEADER_NOT_AVAILABLE: Self = Self(5);
    /// (6) 该 broker 不是当前 leader 或 replica。可重试。
    pub const NOT_LEADER_OR_FOLLOWER: Self = Self(6);
    /// (7) 请求超时。可重试。
    pub const REQUEST_TIMED_OUT: Self = Self(7);
    /// (8) broker 不可用。不可重试。
    pub const BROKER_NOT_AVAILABLE: Self = Self(8);
    /// (9) replica 不可用。可重试。
    pub const REPLICA_NOT_AVAILABLE: Self = Self(9);
    /// (10) 消息超过服务器最大消息大小。不可重试。
    pub const MESSAGE_TOO_LARGE: Self = Self(10);
    /// (11) controller 已迁移到其他 broker。不可重试。
    pub const STALE_CONTROLLER_EPOCH: Self = Self(11);
    /// (12) 偏移量请求的 metadata 字段过大。不可重试。
    pub const OFFSET_METADATA_TOO_LARGE: Self = Self(12);
    /// (13) 在收到响应前服务器断开连接。可重试。
    pub const NETWORK_EXCEPTION: Self = Self(13);
    /// (14) coordinator 正在加载中。可重试。
    pub const COORDINATOR_LOAD_IN_PROGRESS: Self = Self(14);
    /// (15) coordinator 不可用。可重试。
    pub const COORDINATOR_NOT_AVAILABLE: Self = Self(15);
    /// (16) 不是正确的 coordinator。可重试。
    pub const NOT_COORDINATOR: Self = Self(16);
    /// (17) 对无效主题执行操作。不可重试。
    pub const INVALID_TOPIC_EXCEPTION: Self = Self(17);
    /// (18) 消息批次大于服务器配置的 segment 大小。不可重试。
    pub const RECORD_LIST_TOO_LARGE: Self = Self(18);
    /// (19) 同步副本不足。可重试。
    pub const NOT_ENOUGH_REPLICAS: Self = Self(19);
    /// (20) 消息已写入日志但同步副本不足。可重试。
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: Self = Self(20);
    /// (21) required acks 值无效。不可重试。
    pub const INVALID_REQUIRED_ACKS: Self = Self(21);
    /// (22) 指定的 group generation ID 无效。不可重试。
    pub const ILLEGAL_GENERATION: Self = Self(22);
    /// (23) 组成员的支持协议不兼容。不可重试。
    pub const INCONSISTENT_GROUP_PROTOCOL: Self = Self(23);
    /// (24) Group ID 无效。不可重试。
    pub const INVALID_GROUP_ID: Self = Self(24);
    /// (25) coordinator 不感知该成员。不可重试。
    pub const UNKNOWN_MEMBER_ID: Self = Self(25);
    /// (26) session 超时超出范围。不可重试。
    pub const INVALID_SESSION_TIMEOUT: Self = Self(26);
    /// (27) Group 正在 rebalance，需要重新加入。不可重试。
    pub const REBALANCE_IN_PROGRESS: Self = Self(27);
    /// (28) 提交的偏移量数据大小无效。不可重试。
    pub const INVALID_COMMIT_OFFSET_SIZE: Self = Self(28);
    /// (29) 主题授权失败。不可重试。
    pub const TOPIC_AUTHORIZATION_FAILED: Self = Self(29);
    /// (30) Group 授权失败。不可重试。
    pub const GROUP_AUTHORIZATION_FAILED: Self = Self(30);
    /// (31) 集群授权失败。不可重试。
    pub const CLUSTER_AUTHORIZATION_FAILED: Self = Self(31);
    /// (32) 消息时间戳超出可接受范围。不可重试。
    pub const INVALID_TIMESTAMP: Self = Self(32);
    /// (33) 不支持请求的 SASL 机制。不可重试。
    pub const UNSUPPORTED_SASL_MECHANISM: Self = Self(33);
    /// (34) 请求在当前 SASL 状态下无效。不可重试。
    pub const ILLEGAL_SASL_STATE: Self = Self(34);
    /// (35) 不支持的 API 版本。不可重试。
    pub const UNSUPPORTED_VERSION: Self = Self(35);
    /// (36) 主题已存在。不可重试。
    pub const TOPIC_ALREADY_EXISTS: Self = Self(36);
    /// (37) 分区数小于 1。不可重试。
    pub const INVALID_PARTITIONS: Self = Self(37);
    /// (38) 复制因子小于 1 或大于可用 broker 数。不可重试。
    pub const INVALID_REPLICATION_FACTOR: Self = Self(38);
    /// (39) replica 分配无效。不可重试。
    pub const INVALID_REPLICA_ASSIGNMENT: Self = Self(39);
    /// (40) 配置无效。不可重试。
    pub const INVALID_CONFIG: Self = Self(40);
    /// (41) 不是正确的集群 controller。可重试。
    pub const NOT_CONTROLLER: Self = Self(41);
    /// (42) 请求格式错误或发送到不兼容的 broker。不可重试。
    pub const INVALID_REQUEST: Self = Self(42);
    /// (43) broker 上的消息格式版本不支持该请求。不可重试。
    pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: Self = Self(43);
    /// (44) 请求参数违反配置的策略。不可重试。
    pub const POLICY_VIOLATION: Self = Self(44);
    /// (45) broker 收到乱序的 sequence number。不可重试。
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: Self = Self(45);
    /// (46) broker 收到重复的 sequence number。不可重试。
    pub const DUPLICATE_SEQUENCE_NUMBER: Self = Self(46);
    /// (47) producer 使用旧的 epoch 尝试 produce。不可重试。
    pub const INVALID_PRODUCER_EPOCH: Self = Self(47);
    /// (48) producer 在无效状态尝试事务操作。不可重试。
    pub const INVALID_TXN_STATE: Self = Self(48);
    /// (49) producer 使用的 producer ID 未分配给其 transaction ID。不可重试。
    pub const INVALID_PRODUCER_ID_MAPPING: Self = Self(49);
    /// (50) 事务超时超出最大值。不可重试。
    pub const INVALID_TRANSACTION_TIMEOUT: Self = Self(50);
    /// (51) 并发事务冲突。可重试。
    pub const CONCURRENT_TRANSACTIONS: Self = Self(51);
    /// (52) 事务 coordinator 已被隔离。不可重试。
    pub const TRANSACTION_COORDINATOR_FENCED: Self = Self(52);
    /// (53) Transactional ID 授权失败。不可重试。
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: Self = Self(53);
    /// (54) 安全功能已禁用。不可重试。
    pub const SECURITY_DISABLED: Self = Self(54);
    /// (55) broker 未尝试执行该操作。不可重试。
    pub const OPERATION_NOT_ATTEMPTED: Self = Self(55);
    /// (56) 访问磁盘日志文件时出错。可重试。
    pub const KAFKA_STORAGE_ERROR: Self = Self(56);
    /// (57) broker 配置中未找到用户指定的日志目录。不可重试。
    pub const LOG_DIR_NOT_FOUND: Self = Self(57);
    /// (58) SASL 认证失败。不可重试。
    pub const SASL_AUTHENTICATION_FAILED: Self = Self(58);
    /// (59) 找不到 producer ID 关联的元数据。不可重试。
    pub const UNKNOWN_PRODUCER_ID: Self = Self(59);
    /// (60) 分区 reassignment 正在进行中。不可重试。
    pub const REASSIGNMENT_IN_PROGRESS: Self = Self(60);
    /// (61) Delegation Token 特性未启用。不可重试。
    pub const DELEGATION_TOKEN_AUTH_DISABLED: Self = Self(61);
    /// (62) 服务器上找不到 Delegation Token。不可重试。
    pub const DELEGATION_TOKEN_NOT_FOUND: Self = Self(62);
    /// (63) 指定的 Principal 不是有效的 Owner/Renewer。不可重试。
    pub const DELEGATION_TOKEN_OWNER_MISMATCH: Self = Self(63);
    /// (64) PLAINTEXT/1-way SSL 通道上不允许 Delegation Token 请求。不可重试。
    pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: Self = Self(64);
    /// (65) Delegation Token 授权失败。不可重试。
    pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: Self = Self(65);
    /// (66) Delegation Token 已过期。不可重试。
    pub const DELEGATION_TOKEN_EXPIRED: Self = Self(66);
    /// (67) 不支持的 principalType。不可重试。
    pub const INVALID_PRINCIPAL_TYPE: Self = Self(67);
    /// (68) Group 非空。不可重试。
    pub const NON_EMPTY_GROUP: Self = Self(68);
    /// (69) Group ID 不存在。不可重试。
    pub const GROUP_ID_NOT_FOUND: Self = Self(69);
    /// (70) 找不到 fetch session ID。可重试。
    pub const FETCH_SESSION_ID_NOT_FOUND: Self = Self(70);
    /// (71) fetch session epoch 无效。可重试。
    pub const INVALID_FETCH_SESSION_EPOCH: Self = Self(71);
    /// (72) leader broker 上找不到匹配的 listener。可重试。
    pub const LISTENER_NOT_FOUND: Self = Self(72);
    /// (73) 主题删除已禁用。不可重试。
    pub const TOPIC_DELETION_DISABLED: Self = Self(73);
    /// (74) 请求中的 leader epoch 旧于 broker 上的 epoch。可重试。
    pub const FENCED_LEADER_EPOCH: Self = Self(74);
    /// (75) 请求中的 leader epoch 新于 broker 上的 epoch。可重试。
    pub const UNKNOWN_LEADER_EPOCH: Self = Self(75);
    /// (76) 请求客户端不支持给定分区的压缩类型。不可重试。
    pub const UNSUPPORTED_COMPRESSION_TYPE: Self = Self(76);
    /// (77) Broker epoch 已更改。不可重试。
    pub const STALE_BROKER_EPOCH: Self = Self(77);
    /// (78) leader high watermark 尚未追上。可重试。
    pub const OFFSET_NOT_AVAILABLE: Self = Self(78);
    /// (79) 成员需要有有效的 member ID。不可重试。
    pub const MEMBER_ID_REQUIRED: Self = Self(79);
    /// (80) 首选 leader 不可用。可重试。
    pub const PREFERRED_LEADER_NOT_AVAILABLE: Self = Self(80);
    /// (81) group 已达最大大小。不可重试。
    pub const GROUP_MAX_SIZE_REACHED: Self = Self(81);
    /// (82) 具有相同 group.instance.id 的其他 consumer 已注册。不可重试。
    pub const FENCED_INSTANCE_ID: Self = Self(82);
    /// (83) 没有可用的合格主题分区 leader。可重试。
    pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: Self = Self(83);
    /// (84) 主题分区不需要 leader 选举。可重试。
    pub const ELECTION_NOT_NEEDED: Self = Self(84);
    /// (85) 没有分区 reassignment 正在进行。不可重试。
    pub const NO_REASSIGNMENT_IN_PROGRESS: Self = Self(85);
    /// (86) 在 consumer group 活跃订阅时禁止删除主题偏移量。不可重试。
    pub const GROUP_SUBSCRIBED_TO_TOPIC: Self = Self(86);
    /// (87) 记录未通过 broker 验证。不可重试。
    pub const INVALID_RECORD: Self = Self(87);
    /// (88) 存在需要清除的不稳定偏移量。可重试。
    pub const UNSTABLE_OFFSET_COMMIT: Self = Self(88);
    /// (89) 已超过节流配额。可重试。
    pub const THROTTLING_QUOTA_EXCEEDED: Self = Self(89);
    /// (90) 存在具有相同 transactionalId 的更新 producer。不可重试。
    pub const PRODUCER_FENCED: Self = Self(90);
    /// (91) 请求非法引用不存在的资源。不可重试。
    pub const RESOURCE_NOT_FOUND: Self = Self(91);
    /// (92) 请求非法两次引用同一资源。不可重试。
    pub const DUPLICATE_RESOURCE: Self = Self(92);
    /// (93) 请求的凭证不满足可接受性标准。不可重试。
    pub const UNACCEPTABLE_CREDENTIAL: Self = Self(93);
    /// (94) voter 请求的发送者或接收者不是预期的投票者。不可重试。
    pub const INCONSISTENT_VOTER_SET: Self = Self(94);
    /// (95) 给定的更新版本无效。不可重试。
    pub const INVALID_UPDATE_VERSION: Self = Self(95);
    /// (96) 由于意外的服务器错误无法更新 finalized features。不可重试。
    pub const FEATURE_UPDATE_FAILED: Self = Self(96);
    /// (97) 请求 principal 反序列化失败。不可重试。
    pub const PRINCIPAL_DESERIALIZATION_FAILURE: Self = Self(97);
    /// (98) 请求的 snapshot 未找到。不可重试。
    pub const SNAPSHOT_NOT_FOUND: Self = Self(98);
    /// (99) 请求的位置无效。不可重试。
    pub const POSITION_OUT_OF_RANGE: Self = Self(99);
    /// (100) 该服务器不托管此主题 ID。可重试。
    pub const UNKNOWN_TOPIC_ID: Self = Self(100);
    /// (101) 此 broker ID 已被使用。不可重试。
    pub const DUPLICATE_BROKER_REGISTRATION: Self = Self(101);
    /// (102) 给定的 broker ID 未注册。不可重试。
    pub const BROKER_ID_NOT_REGISTERED: Self = Self(102);
    /// (103) 日志的主题 ID 与请求中的主题 ID 不匹配。可重试。
    pub const INCONSISTENT_TOPIC_ID: Self = Self(103);
    /// (104) 请求中的 clusterId 与服务器上的不匹配。不可重试。
    pub const INCONSISTENT_CLUSTER_ID: Self = Self(104);
    /// (105) 找不到 transactionalId。不可重试。
    pub const TRANSACTIONAL_ID_NOT_FOUND: Self = Self(105);
    /// (106) fetch session 遇到不一致的主题 ID 使用。可重试。
    pub const FETCH_SESSION_TOPIC_ID_ERROR: Self = Self(106);
    /// (107) 新 ISR 包含至少一个不合格的 replica。不可重试。
    pub const INELIGIBLE_REPLICA: Self = Self(107);
    /// (108) AlterPartition 成功更新分区状态但 leader 已更改。不可重试。
    pub const NEW_LEADER_ELECTED: Self = Self(108);
    /// (109) 请求的偏移量已移至分层存储。不可重试。
    pub const OFFSET_MOVED_TO_TIERED_STORAGE: Self = Self(109);
    /// (110) 成员 epoch 被 group coordinator 隔离。不可重试。
    pub const FENCED_MEMBER_EPOCH: Self = Self(110);
    /// (111) instance ID 仍被 consumer group 中另一成员使用。不可重试。
    pub const UNRELEASED_INSTANCE_ID: Self = Self(111);
    /// (112) 分配器或其版本范围不受 consumer group 支持。不可重试。
    pub const UNSUPPORTED_ASSIGNOR: Self = Self(112);
    /// (113) 成员 epoch 已过时。不可重试。
    pub const STALE_MEMBER_EPOCH: Self = Self(113);
    /// (114) 请求被发送到错误类型的端点。不可重试。
    pub const MISMATCHED_ENDPOINT_TYPE: Self = Self(114);
    /// (115) 尚不支持此端点类型。不可重试。
    pub const UNSUPPORTED_ENDPOINT_TYPE: Self = Self(115);
    /// (116) 此 controller ID 未知。不可重试。
    pub const UNKNOWN_CONTROLLER_ID: Self = Self(116);
    /// (117) 客户端发送了无效或过期的 subscription ID。不可重试。
    pub const UNKNOWN_SUBSCRIPTION_ID: Self = Self(117);
    /// (118) 客户端发送的 push telemetry 请求过大。不可重试。
    pub const TELEMETRY_TOO_LARGE: Self = Self(118);
    /// (119) controller 认为 broker 注册无效。不可重试。
    pub const INVALID_REGISTRATION: Self = Self(119);
    /// (120) 服务器遇到事务错误。不可重试。
    pub const TRANSACTION_ABORTABLE: Self = Self(120);
    /// (121) 记录状态无效。不可重试。
    pub const INVALID_RECORD_STATE: Self = Self(121);
    /// (122) 找不到 share session。可重试。
    pub const SHARE_SESSION_NOT_FOUND: Self = Self(122);
    /// (123) share session epoch 无效。可重试。
    pub const INVALID_SHARE_SESSION_EPOCH: Self = Self(123);
    /// (124) share-group state epoch 不匹配。不可重试。
    pub const FENCED_STATE_EPOCH: Self = Self(124);
    /// (125) voter key 与接收 replica 的 key 不匹配。不可重试。
    pub const INVALID_VOTER_KEY: Self = Self(125);
    /// (126) voter 已存在于 voter 集合中。不可重试。
    pub const DUPLICATE_VOTER: Self = Self(126);
    /// (127) voter 不在 voter 集合中。不可重试。
    pub const VOTER_NOT_FOUND: Self = Self(127);
    /// (128) 正则表达式无效。不可重试。
    pub const INVALID_REGULAR_EXPRESSION: Self = Self(128);
    /// (129) 客户端元数据已过时，需要重新引导。不可重试。
    pub const REBOOTSTRAP_REQUIRED: Self = Self(129);
    /// (130) 提供的拓扑无效。不可重试。
    pub const STREAMS_INVALID_TOPOLOGY: Self = Self(130);
    /// (131) 提供的拓扑 epoch 无效。不可重试。
    pub const STREAMS_INVALID_TOPOLOGY_EPOCH: Self = Self(131);
    /// (132) 提供的拓扑 epoch 已过时。不可重试。
    pub const STREAMS_TOPOLOGY_FENCED: Self = Self(132);
    /// (133) 已达到 share session 限制。可重试。
    pub const SHARE_SESSION_LIMIT_REACHED: Self = Self(133);

    // =======================================================================
    // 方法
    // =======================================================================

    /// 从 `i16` 错误码创建 `KafkaErrorCode`。
    #[inline]
    pub const fn from_i16(code: i16) -> Self {
        Self(code)
    }

    /// 获取原始错误码值。
    #[inline]
    pub const fn code(&self) -> i16 {
        self.0
    }

    /// 判断该错误码是否表示无错误。
    #[inline]
    pub const fn is_ok(&self) -> bool {
        self.0 == 0
    }

    /// 判断该错误是否可重试。
    pub fn is_retriable(&self) -> bool {
        match self.0 {
            2  // CORRUPT_MESSAGE
            | 3  // UNKNOWN_TOPIC_OR_PARTITION
            | 5  // LEADER_NOT_AVAILABLE
            | 6  // NOT_LEADER_OR_FOLLOWER
            | 7  // REQUEST_TIMED_OUT
            | 9  // REPLICA_NOT_AVAILABLE
            | 13 // NETWORK_EXCEPTION
            | 14 // COORDINATOR_LOAD_IN_PROGRESS
            | 15 // COORDINATOR_NOT_AVAILABLE
            | 16 // NOT_COORDINATOR
            | 19 // NOT_ENOUGH_REPLICAS
            | 20 // NOT_ENOUGH_REPLICAS_AFTER_APPEND
            | 41 // NOT_CONTROLLER
            | 51 // CONCURRENT_TRANSACTIONS
            | 56 // KAFKA_STORAGE_ERROR
            | 70 // FETCH_SESSION_ID_NOT_FOUND
            | 71 // INVALID_FETCH_SESSION_EPOCH
            | 72 // LISTENER_NOT_FOUND
            | 74 // FENCED_LEADER_EPOCH
            | 75 // UNKNOWN_LEADER_EPOCH
            | 78 // OFFSET_NOT_AVAILABLE
            | 80 // PREFERRED_LEADER_NOT_AVAILABLE
            | 83 // ELIGIBLE_LEADERS_NOT_AVAILABLE
            | 84 // ELECTION_NOT_NEEDED
            | 88 // UNSTABLE_OFFSET_COMMIT
            | 89 // THROTTLING_QUOTA_EXCEEDED
            | 100 // UNKNOWN_TOPIC_ID
            | 103 // INCONSISTENT_TOPIC_ID
            | 106 // FETCH_SESSION_TOPIC_ID_ERROR
            | 122 // SHARE_SESSION_NOT_FOUND
            | 123 // INVALID_SHARE_SESSION_EPOCH
            | 133 => true, // SHARE_SESSION_LIMIT_REACHED
            _ => false,
        }
    }

    /// 获取错误码对应的描述文本。
    pub fn description(&self) -> &'static str {
        match self.0 {
            -1 => "The server experienced an unexpected error when processing the request",
            0 => "",
            1 => "The requested offset is not within the range of offsets maintained by the server",
            2 => {
                "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt"
            }
            3 => "This server does not host this topic-partition",
            4 => "The requested fetch size is invalid",
            5 => {
                "There is no leader for this topic-partition as we are in the middle of a leadership election"
            }
            6 => {
                "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition"
            }
            7 => "The request timed out",
            8 => "The broker is not available",
            9 => "The replica is not available for the requested topic-partition",
            10 => {
                "The request included a message larger than the max message size the server will accept"
            }
            11 => "The controller moved to another broker",
            12 => "The metadata field of the offset request was too large",
            13 => "The server disconnected before a response was received",
            14 => "The coordinator is loading and hence can't process requests",
            15 => "The coordinator is not available",
            16 => "This is not the correct coordinator",
            17 => "The request attempted to perform an operation on an invalid topic",
            18 => {
                "The request included message batch larger than the configured segment size on the server"
            }
            19 => "Messages are rejected since there are fewer in-sync replicas than required",
            20 => "Messages are written to the log, but to fewer in-sync replicas than required",
            21 => "Produce request specified an invalid value for required acks",
            22 => "Specified group generation id is not valid",
            23 => {
                "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list"
            }
            24 => "The group id is invalid",
            25 => "The coordinator is not aware of this member",
            26 => "The session timeout is not within the range allowed by the broker",
            27 => "The group is rebalancing, so a rejoin is needed",
            28 => "The committing offset data size is not valid",
            29 => "Topic authorization failed",
            30 => "Group authorization failed",
            31 => "Cluster authorization failed",
            32 => "The timestamp of the message is out of acceptable range",
            33 => "The broker does not support the requested SASL mechanism",
            34 => "Request is not valid given the current SASL state",
            35 => "The version of API is not supported",
            36 => "Topic with this name already exists",
            37 => "Number of partitions is below 1",
            38 => "Replication factor is below 1 or larger than the number of available brokers",
            39 => "Replica assignment is invalid",
            40 => "Configuration is invalid",
            41 => "This is not the correct controller for this cluster",
            42 => {
                "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker"
            }
            43 => "The message format version on the broker does not support the request",
            44 => "Request parameters do not satisfy the configured policy",
            45 => "The broker received an out of order sequence number",
            46 => "The broker received a duplicate sequence number",
            47 => "Producer attempted to produce with an old epoch",
            48 => "The producer attempted a transactional operation in an invalid state",
            49 => {
                "The producer attempted to use a producer id which is not currently assigned to its transactional id"
            }
            50 => "The transaction timeout is larger than the maximum value allowed by the broker",
            51 => {
                "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing"
            }
            52 => {
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer"
            }
            53 => "Transactional Id authorization failed",
            54 => "Security features are disabled",
            55 => "The broker did not attempt to execute this operation",
            56 => "Disk error when trying to access log file on the disk",
            57 => "The user-specified log directory is not found in the broker config",
            58 => "SASL Authentication failed",
            59 => {
                "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question"
            }
            60 => "A partition reassignment is in progress",
            61 => "Delegation Token feature is not enabled",
            62 => "Delegation Token is not found on server",
            63 => "Specified Principal is not valid Owner/Renewer",
            64 => {
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels"
            }
            65 => "Delegation Token authorization failed",
            66 => "Delegation Token is expired",
            67 => "Supplied principalType is not supported",
            68 => "The group is not empty",
            69 => "The group id does not exist",
            70 => "The fetch session ID was not found",
            71 => "The fetch session epoch is invalid",
            72 => {
                "There is no listener on the leader broker that matches the listener on which metadata request was processed"
            }
            73 => "Topic deletion is disabled",
            74 => "The leader epoch in the request is older than the epoch on the broker",
            75 => "The leader epoch in the request is newer than the epoch on the broker",
            76 => "The requesting client does not support the compression type of given partition",
            77 => "Broker epoch has changed",
            78 => {
                "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing"
            }
            79 => {
                "The group member needs to have a valid member id before actually entering a consumer group"
            }
            80 => "The preferred leader was not available",
            81 => "The group has reached its maximum size",
            82 => {
                "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id"
            }
            83 => "Eligible topic partition leaders are not available",
            84 => "Leader election not needed for topic partition",
            85 => "No partition reassignment is in progress",
            86 => {
                "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it"
            }
            87 => "This record has failed the validation on broker and hence will be rejected",
            88 => "There are unstable offsets that need to be cleared",
            89 => "The throttling quota has been exceeded",
            90 => {
                "There is a newer producer with the same transactionalId which fences the current one"
            }
            91 => "A request illegally referred to a resource that does not exist",
            92 => "A request illegally referred to the same resource twice",
            93 => "Requested credential would not meet criteria for acceptability",
            94 => {
                "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters"
            }
            95 => "The given update version was invalid",
            96 => "Unable to update finalized features due to an unexpected server error",
            97 => "Request principal deserialization failed during forwarding",
            98 => "Requested snapshot was not found",
            99 => {
                "Requested position is not greater than or equal to zero, and less than the size of the snapshot"
            }
            100 => "This server does not host this topic ID",
            101 => "This broker ID is already in use",
            102 => "The given broker ID was not registered",
            103 => "The log's topic ID did not match the topic ID in the request",
            104 => "The clusterId in the request does not match that found on the server",
            105 => "The transactionalId could not be found",
            106 => "The fetch session encountered inconsistent topic ID usage",
            107 => "The new ISR contains at least one ineligible replica",
            108 => {
                "The AlterPartition request successfully updated the partition state but the leader has changed"
            }
            109 => "The requested offset is moved to tiered storage",
            110 => {
                "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin"
            }
            111 => {
                "The instance ID is still used by another member in the consumer group. That member must leave first"
            }
            112 => "The assignor or its version range is not supported by the consumer group",
            113 => {
                "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API"
            }
            114 => "The request was sent to an endpoint of the wrong type",
            115 => "This endpoint type is not supported yet",
            116 => "This controller ID is not known",
            117 => {
                "Client sent a push telemetry request with an invalid or outdated subscription ID"
            }
            118 => {
                "Client sent a push telemetry request larger than the maximum size the broker will accept"
            }
            119 => "The controller has considered the broker registration to be invalid",
            120 => {
                "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID"
            }
            121 => {
                "The record state is invalid. The acknowledgement of delivery could not be completed"
            }
            122 => "The share session was not found",
            123 => "The share session epoch is invalid",
            124 => {
                "The share coordinator rejected the request because the share-group state epoch did not match"
            }
            125 => "The voter key doesn't match the receiving replica's key",
            126 => "The voter is already part of the set of voters",
            127 => "The voter is not part of the set of voters",
            128 => "The regular expression is not valid",
            129 => "Client metadata is stale. The client should rebootstrap to obtain new metadata",
            130 => "The supplied topology is invalid",
            131 => "The supplied topology epoch is invalid",
            132 => "The supplied topology epoch is outdated",
            133 => "The limit of share sessions has been reached",
            _ => "Unknown error code",
        }
    }
}

impl From<i16> for KafkaErrorCode {
    #[inline]
    fn from(code: i16) -> Self {
        Self(code)
    }
}

impl From<KafkaErrorCode> for i16 {
    #[inline]
    fn from(code: KafkaErrorCode) -> Self {
        code.0
    }
}

impl std::fmt::Display for KafkaErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self.0 {
            -1 => "UNKNOWN_SERVER_ERROR",
            0 => "NONE",
            1 => "OFFSET_OUT_OF_RANGE",
            2 => "CORRUPT_MESSAGE",
            3 => "UNKNOWN_TOPIC_OR_PARTITION",
            4 => "INVALID_FETCH_SIZE",
            5 => "LEADER_NOT_AVAILABLE",
            6 => "NOT_LEADER_OR_FOLLOWER",
            7 => "REQUEST_TIMED_OUT",
            8 => "BROKER_NOT_AVAILABLE",
            9 => "REPLICA_NOT_AVAILABLE",
            10 => "MESSAGE_TOO_LARGE",
            11 => "STALE_CONTROLLER_EPOCH",
            12 => "OFFSET_METADATA_TOO_LARGE",
            13 => "NETWORK_EXCEPTION",
            14 => "COORDINATOR_LOAD_IN_PROGRESS",
            15 => "COORDINATOR_NOT_AVAILABLE",
            16 => "NOT_COORDINATOR",
            17 => "INVALID_TOPIC_EXCEPTION",
            18 => "RECORD_LIST_TOO_LARGE",
            19 => "NOT_ENOUGH_REPLICAS",
            20 => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
            21 => "INVALID_REQUIRED_ACKS",
            22 => "ILLEGAL_GENERATION",
            23 => "INCONSISTENT_GROUP_PROTOCOL",
            24 => "INVALID_GROUP_ID",
            25 => "UNKNOWN_MEMBER_ID",
            26 => "INVALID_SESSION_TIMEOUT",
            27 => "REBALANCE_IN_PROGRESS",
            28 => "INVALID_COMMIT_OFFSET_SIZE",
            29 => "TOPIC_AUTHORIZATION_FAILED",
            30 => "GROUP_AUTHORIZATION_FAILED",
            31 => "CLUSTER_AUTHORIZATION_FAILED",
            32 => "INVALID_TIMESTAMP",
            33 => "UNSUPPORTED_SASL_MECHANISM",
            34 => "ILLEGAL_SASL_STATE",
            35 => "UNSUPPORTED_VERSION",
            36 => "TOPIC_ALREADY_EXISTS",
            37 => "INVALID_PARTITIONS",
            38 => "INVALID_REPLICATION_FACTOR",
            39 => "INVALID_REPLICA_ASSIGNMENT",
            40 => "INVALID_CONFIG",
            41 => "NOT_CONTROLLER",
            42 => "INVALID_REQUEST",
            43 => "UNSUPPORTED_FOR_MESSAGE_FORMAT",
            44 => "POLICY_VIOLATION",
            45 => "OUT_OF_ORDER_SEQUENCE_NUMBER",
            46 => "DUPLICATE_SEQUENCE_NUMBER",
            47 => "INVALID_PRODUCER_EPOCH",
            48 => "INVALID_TXN_STATE",
            49 => "INVALID_PRODUCER_ID_MAPPING",
            50 => "INVALID_TRANSACTION_TIMEOUT",
            51 => "CONCURRENT_TRANSACTIONS",
            52 => "TRANSACTION_COORDINATOR_FENCED",
            53 => "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            54 => "SECURITY_DISABLED",
            55 => "OPERATION_NOT_ATTEMPTED",
            56 => "KAFKA_STORAGE_ERROR",
            57 => "LOG_DIR_NOT_FOUND",
            58 => "SASL_AUTHENTICATION_FAILED",
            59 => "UNKNOWN_PRODUCER_ID",
            60 => "REASSIGNMENT_IN_PROGRESS",
            61 => "DELEGATION_TOKEN_AUTH_DISABLED",
            62 => "DELEGATION_TOKEN_NOT_FOUND",
            63 => "DELEGATION_TOKEN_OWNER_MISMATCH",
            64 => "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
            65 => "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
            66 => "DELEGATION_TOKEN_EXPIRED",
            67 => "INVALID_PRINCIPAL_TYPE",
            68 => "NON_EMPTY_GROUP",
            69 => "GROUP_ID_NOT_FOUND",
            70 => "FETCH_SESSION_ID_NOT_FOUND",
            71 => "INVALID_FETCH_SESSION_EPOCH",
            72 => "LISTENER_NOT_FOUND",
            73 => "TOPIC_DELETION_DISABLED",
            74 => "FENCED_LEADER_EPOCH",
            75 => "UNKNOWN_LEADER_EPOCH",
            76 => "UNSUPPORTED_COMPRESSION_TYPE",
            77 => "STALE_BROKER_EPOCH",
            78 => "OFFSET_NOT_AVAILABLE",
            79 => "MEMBER_ID_REQUIRED",
            80 => "PREFERRED_LEADER_NOT_AVAILABLE",
            81 => "GROUP_MAX_SIZE_REACHED",
            82 => "FENCED_INSTANCE_ID",
            83 => "ELIGIBLE_LEADERS_NOT_AVAILABLE",
            84 => "ELECTION_NOT_NEEDED",
            85 => "NO_REASSIGNMENT_IN_PROGRESS",
            86 => "GROUP_SUBSCRIBED_TO_TOPIC",
            87 => "INVALID_RECORD",
            88 => "UNSTABLE_OFFSET_COMMIT",
            89 => "THROTTLING_QUOTA_EXCEEDED",
            90 => "PRODUCER_FENCED",
            91 => "RESOURCE_NOT_FOUND",
            92 => "DUPLICATE_RESOURCE",
            93 => "UNACCEPTABLE_CREDENTIAL",
            94 => "INCONSISTENT_VOTER_SET",
            95 => "INVALID_UPDATE_VERSION",
            96 => "FEATURE_UPDATE_FAILED",
            97 => "PRINCIPAL_DESERIALIZATION_FAILURE",
            98 => "SNAPSHOT_NOT_FOUND",
            99 => "POSITION_OUT_OF_RANGE",
            100 => "UNKNOWN_TOPIC_ID",
            101 => "DUPLICATE_BROKER_REGISTRATION",
            102 => "BROKER_ID_NOT_REGISTERED",
            103 => "INCONSISTENT_TOPIC_ID",
            104 => "INCONSISTENT_CLUSTER_ID",
            105 => "TRANSACTIONAL_ID_NOT_FOUND",
            106 => "FETCH_SESSION_TOPIC_ID_ERROR",
            107 => "INELIGIBLE_REPLICA",
            108 => "NEW_LEADER_ELECTED",
            109 => "OFFSET_MOVED_TO_TIERED_STORAGE",
            110 => "FENCED_MEMBER_EPOCH",
            111 => "UNRELEASED_INSTANCE_ID",
            112 => "UNSUPPORTED_ASSIGNOR",
            113 => "STALE_MEMBER_EPOCH",
            114 => "MISMATCHED_ENDPOINT_TYPE",
            115 => "UNSUPPORTED_ENDPOINT_TYPE",
            116 => "UNKNOWN_CONTROLLER_ID",
            117 => "UNKNOWN_SUBSCRIPTION_ID",
            118 => "TELEMETRY_TOO_LARGE",
            119 => "INVALID_REGISTRATION",
            120 => "TRANSACTION_ABORTABLE",
            121 => "INVALID_RECORD_STATE",
            122 => "SHARE_SESSION_NOT_FOUND",
            123 => "INVALID_SHARE_SESSION_EPOCH",
            124 => "FENCED_STATE_EPOCH",
            125 => "INVALID_VOTER_KEY",
            126 => "DUPLICATE_VOTER",
            127 => "VOTER_NOT_FOUND",
            128 => "INVALID_REGULAR_EXPRESSION",
            129 => "REBOOTSTRAP_REQUIRED",
            130 => "STREAMS_INVALID_TOPOLOGY",
            131 => "STREAMS_INVALID_TOPOLOGY_EPOCH",
            132 => "STREAMS_TOPOLOGY_FENCED",
            133 => "SHARE_SESSION_LIMIT_REACHED",
            _ => "UNKNOWN",
        };
        write!(f, "{} ({})", name, self.0)
    }
}

// ============ 便捷宏 ============

/// 确保缓冲区有足够的数据
#[macro_export]
macro_rules! ensure_remaining {
    ($buf:expr, $len:expr) => {
        if $buf.remaining() < $len {
            return Err($crate::ProtocolError::insufficient_data(
                $len,
                $buf.remaining(),
            ));
        }
    };
}

/// 确保条件成立，否则返回错误
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
}

// ============ 错误转换 ============

impl From<std::str::Utf8Error> for ProtocolError {
    fn from(err: std::str::Utf8Error) -> Self {
        ProtocolError::InvalidData(format!("Invalid UTF-8: {}", err))
    }
}

// impl From<serde_json::Error> for ProtocolError {
//     fn from(err: serde_json::Error) -> Self {
//         ProtocolError::InvalidData(format!("JSON error: {}", err))
//     }
// }

// impl From<base64::DecodeError> for ProtocolError {
//     fn from(err: base64::DecodeError) -> Self {
//         ProtocolError::InvalidData(format!("Base64 decode error: {}", err))
//     }
// }

impl From<&str> for ProtocolError {
    fn from(s: &str) -> Self {
        ProtocolError::Other(s.to_string())
    }
}

impl From<String> for ProtocolError {
    fn from(s: String) -> Self {
        ProtocolError::Other(s)
    }
}

// ============ 测试 ============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = ProtocolError::invalid_data("test error");
        assert!(matches!(err, ProtocolError::InvalidData(_)));
        assert_eq!(err.error_code(), Some(KafkaErrorCode::INVALID_REQUEST));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_insufficient_data() {
        let err = ProtocolError::insufficient_data(100, 50);
        assert!(matches!(
            err,
            ProtocolError::InsufficientData {
                expected: 100,
                actual: 50
            }
        ));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_unsupported_version() {
        let err = ProtocolError::unsupported_version(3, 99);
        assert!(matches!(
            err,
            ProtocolError::UnsupportedVersion {
                api_key: 3,
                version: 99
            }
        ));
        assert_eq!(err.error_code(), Some(KafkaErrorCode::UNSUPPORTED_VERSION));
    }
}
