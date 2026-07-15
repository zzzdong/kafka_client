//! Kerberos protocol constants and build helpers.
//!
//! This module provides protocol constants from Kerberos (RFC 4120). **GSS-API layer token
//! wrapping and AP-REQ/AP-REP construction have been moved to the crate-level `gss` module**.

// ===========================================================================
// Kerberos protocol constants (RFC 4120)
// ===========================================================================

pub const PVNO: i32 = 5;

/// Message types
pub const MSG_AS_REQ: i32 = 10;
pub const MSG_AS_REP: i32 = 11;
pub const MSG_TGS_REQ: i32 = 12;
pub const MSG_TGS_REP: i32 = 13;

/// PA-DATA types
pub const PADATA_TGS_REQ: i32 = 1;
pub const PADATA_ENC_TIMESTAMP: i32 = 2;

/// Name types
pub const NT_PRINCIPAL: i32 = 1;
pub const NT_SRV_INST: i32 = 2;
pub const NT_SRV_HST: i32 = 3;

/// APPLICATION tag offsets: [APPLICATION n] = 0x60 | n
pub const TAG_AS_REQ: u8 = 0x6A; // [APPLICATION 10]
pub const TAG_AS_REP: u8 = 0x6B; // [APPLICATION 11]
pub const TAG_TGS_REQ: u8 = 0x6C; // [APPLICATION 12]
pub const TAG_TGS_REP: u8 = 0x6D; // [APPLICATION 13]
pub const TAG_ENC_AS_REP_PART: u8 = 0x79; // [APPLICATION 25]
pub const TAG_ENC_TGS_REP_PART: u8 = 0x7A; // [APPLICATION 26]

/// 密钥用法 (RFC 4120 §7.5.1)
pub const KEY_USAGE_PA_ENC_TIMESTAMP: u32 = 1;
pub const KEY_USAGE_AS_REP_ENC_PART: u32 = 3;
pub const KEY_USAGE_TGS_REQ_PA_TGS_REQ: u32 = 7;
#[allow(dead_code)]
pub const KEY_USAGE_TGS_REP_ENC_PART: u32 = 8;

/// 密钥用法 — AP-REQ Authenticator (RFC 4120 §7.5.1, 编号 11)
pub const KEY_USAGE_AP_REQ_AUTH: u32 = 11;
/// 密钥用法 — AP-REP enc-part (RFC 4120 §7.5.1, 编号 12)
pub const KEY_USAGE_AP_REP_ENC_PART: u32 = 12;
