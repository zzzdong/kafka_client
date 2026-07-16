//! Pure Rust Kerberos protocol engine + krb5 GSS-API mechanism (initiator).
//!
//! This crate provides two major capabilities, both independent of any specific
//! service (e.g. Kafka), and can be used standalone in any Kerberos KDC scenario:
//!
//! ## 1. Kerberos KDC Protocol Engine
//!
//! - [`KerberosCredentials`]: Pure Rust credential model (principal / service_name / keytab).
//! - [`KerberosClient`]: KDC protocol state machine that communicates with KDC via
//!   [`KdcTransport`] to perform AS-REQ → AS-REP → TGS-REQ → TGS-REP, producing
//!   [`AcquiredTicket`] (service ticket + session key).
//! - [`Krb5Client`]: High-level KDC client facade that holds credentials + transport
//!   for direct KDC access without needing to inject transport on every call.
//! - [`KdcTransport`]: KDC transport contract; integrators implement it, or use the
//!   built-in [`TokioKdcTransport`].
//! - [`TokioKdcTransport`]: Out-of-the-box KDC transport based on tokio `TcpStream`.
//!
//! ## 2. krb5 GSS-API Mechanism (initiator)
//!
//! - [`gss`] module: Implements krb5 GSS context (AP-REQ / AP-REP construction,
//!   GSS token wrapping, RFC 4121 WRAP, multi-round handshake) based on
//!   [`AcquiredTicket`], compatible with any SASL/GSSAPI server-first service
//!   (Kafka, LDAP, etc.).
//! - [`gss::GssClient`]: High-level GSS facade combining KDC ticket acquisition + GSS
//!   context for single-call access. Pair with [`gss::GssContext::step`] for the
//!   standard GSS handshake loop compatible with any acceptor.
//!
//! Cryptography is built on RustCrypto primitives (`aes` / `cipher` / `hmac` / `sha1` / `sha2`)
//! with zero FFI, enabling cross-compilation to any target. Network I/O is abstracted
//! via the [`KdcTransport`] trait; the built-in [`TokioKdcTransport`] (tokio-based) can
//! connect to a KDC directly, or integrators may implement the trait themselves.
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
#[cfg(feature = "tokio-transport")]
pub use kerberos::transport::TokioKdcTransport;
pub use kerberos::transport::{KdcBoxFuture, KdcTransport};
