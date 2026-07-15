//! Pure Rust Kerberos protocol engine + krb5 GSS-API mechanism (initiator).
//!
//! 该 crate 提供两大能力, 二者均不依赖任何特定服务 (如 Kafka), 可独立用于任意
//! 需要与 Kerberos KDC 对接的场景:
//!
//! ## 1. Kerberos KDC 协议引擎
//!
//! - [`KerberosCredentials`]: 纯 Rust 的 Kerberos 凭证模型 (principal / service_name / keytab)。
//! - [`KerberosClient`]: KDC 协议状态机, 通过 [`KdcTransport`] 与 KDC 通信完成
//!   AS-REQ → AS-REP → TGS-REQ → TGS-REP, 最终产出 [`AcquiredTicket`] (服务票据 + 会话密钥)。
//! - [`Krb5Client`]: 高层 KDC 客户端门面, 持有凭证 + 传输, 以 client 方式直接访问 KDC,
//!   无需每次调用都注入 transport。
//! - [`KdcTransport`]: KDC 传输契约, 集成方实现或使用内置的 [`TokioKdcTransport`]。
//! - [`TokioKdcTransport`]: 基于 tokio TcpStream 的开箱即用 KDC 传输 (crate 默认提供)。
//!
//! ## 2. krb5 GSS-API 机制 (initiator)
//!
//! - [`gss`] 模块: 基于 [`AcquiredTicket`] 实现 krb5 GSS 上下文 (AP-REQ / AP-REP 构建、
//!   GSS 令牌封装、RFC 4121 WRAP、多轮握手驱动), 可与任意 SASL/GSSAPI server-first
//!   服务 (Kafka、LDAP 等) 对接。
//! - [`gss::GssClient`]: 高层 GSS 门面, 组合 KDC 取票 + 上下文, 对外"一键式"接入;
//!   配合 [`gss::GssContext::step`] 标准 GSS 循环即可完成与任意 acceptor 的握手。
//!
//! 密码学层基于 RustCrypto 基元 (`aes` / `cipher` / `hmac` / `sha1` / `sha2`) 构建, 无 FFI,
//! 任意 target 可直接交叉编译。crate 通过 [`KdcTransport`] 抽象网络收发, 内置 [`TokioKdcTransport`]
//! (基于 tokio) 可直接连接 KDC, 也可由集成方自行实现该 trait。
pub mod client;
pub mod credentials;
pub mod error;
pub mod gss;
pub mod kerberos;

pub use client::Krb5Client;
pub use credentials::KerberosCredentials;
pub use error::{KerberosError, Result};
pub use gss::GssClient;
pub use kerberos::client::{AcquiredTicket, KerberosClient};
pub use kerberos::crypto::Etype;
pub use kerberos::transport::TokioKdcTransport;
pub use kerberos::transport::{KdcBoxFuture, KdcTransport};
