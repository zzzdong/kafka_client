//! GSS-API layer (krb5 mechanism): SASL/GSSAPI client authentication based on Kerberos service tickets.
//!
//! This module provides a pure-Rust krb5 GSS mechanism initiator that can interop with any
//! server-first multi-round SASL/GSSAPI service (e.g. Kafka, LDAP over GSSAPI),
//! and is not tied to any specific service.
//!
//! # 架构
//!
//! - [`GssContext`] trait: interaction interface aligned with SASL/GSSAPI server-first multi-round exchange
//! - [`NativeGssContext`]: implementation based on `krb5-gss` KDC tickets
//! - [`acquire_gss_context`]: async factory: KDC ticket acquisition → context creation
//!
//! # Handshake flow (Kafka example)
//!
//! ```text
//! Client                                      Acceptor (e.g. Kafka Broker)
//!   │                                              │
//!   │── SaslHandshakeRequest("GSSAPI") ───────────→│
//!   │←── SaslHandshakeResponse(OK) ───────────────│
//!   │── SaslAuthenticateRequest(AP-REQ) ──────────→│   ← initial_token()
//!   │←── SaslAuthenticateResponse(AP-REP) ────────│   ← handle_challenge()
//!   │── SaslAuthenticateRequest(empty) ───────────→│   ← 收尾确认
//!   │←── SaslAuthenticateResponse(OK) ────────────│
//! ```
//!
//! GSS 令牌格式 (RFC 1964 / RFC 4121)：
//! ```text
//! GSS-API InitialContextToken ::=
//!   [APPLICATION 0] IMPLICIT SEQUENCE {
//!     thisMech  OID (krb5 = 1.2.840.113554.1.2.2),
//!     innerToken AP-REQ / AP-REP DER
//!   }
//! ```

use crate::KerberosCredentials;
use crate::client::Krb5Client;
use crate::error::{KerberosError, Result};
use crate::kerberos::asn1::{
    Authenticator, EncApRepPart, EncryptedData, EncryptionKey, PrincipalName, Ticket, ctx_int,
    decode_ap_rep, decode_ap_req, decode_enc_ap_rep_part, der_len, encode_ap_req,
    encode_authenticator, read_len, tlv, tlv_ctx, tlv_seq,
};
use crate::kerberos::client::{AcquiredTicket, KerberosClient};
use crate::kerberos::crypto::{self, Etype};
use crate::kerberos::messages::{KEY_USAGE_AP_REP_ENC_PART, KEY_USAGE_AP_REQ_AUTH};
use crate::kerberos::transport::KdcTransport;
use crate::kerberos::util::{now_micros, utc_now_generalized};

// ===========================================================================
// GSS 令牌封装 (RFC 1964 / RFC 4121 / RFC 2743)
// ===========================================================================

// RFC 4121 GSS 安全层密钥用法 (per-message tokens):
//   GSS_USAGE_ACCEPTOR_SEAL = 22 (服务端密封/加密)
//   GSS_USAGE_ACCEPTOR_SIGN = 23 (服务端签名)
//   GSS_USAGE_INITIATOR_SEAL = 24 (客户端密封/加密)
//   GSS_USAGE_INITIATOR_SIGN = 25 (客户端签名)
#[allow(dead_code)]
const GSS_USAGE_ACCEPTOR_SIGN: u32 = 23;
const GSS_USAGE_ACCEPTOR_SEAL: u32 = 22;
#[allow(dead_code)]
const GSS_USAGE_INITIATOR_SIGN: u32 = 25;
const GSS_USAGE_INITIATOR_SEAL: u32 = 24;

// RFC 4121 GSS_WRAP token 结构 (CFX v3, non-confidential wrap):
//
// 非保密性 WRAP token (conf_req_flag=false):
//
//   Header (16 bytes):
//     [0-1]  TOKEN_ID  = 0x0504 (big-endian u16)
//     [2]    flags:
//            bit0: FLAG_SENDER_IS_ACCEPTOR (0x01)
//            bit1: FLAG_WRAP_CONFIDENTIAL  (0x02)
//            bit2: FLAG_ACCEPTOR_SUBKEY    (0x04)
//     [3]    filler = 0xFF
//     [4-5]  EC = checksum size (big-endian u16)
//     [6-7]  RRC (big-endian u16, rotation count, 0 for non-encrypted)
//     [8-15] sequence number (big-endian u64)
//
//   Body:
//     [16..16+N]   payload (N bytes)
//     [16+N..]     checksum (EC bytes)
//
// Checksum = HMAC-SHA1-96(Kc, payload || header_with_EC=0)[0..EC]
//   where Kc = DK(session_key, usage | 0x99) per RFC 3961 §5.3
//
// MIT krb5 的 key_usage: ACCEPTOR_SEAL=22, INITIATOR_SEAL=24.
// DK 使用 AES-驱动的 DR (dk_aes_aes, nfold pattern)，与 MIT krb5 兼容.

/// Unwrap an RFC 4121 GSS_WRAP token sent by the acceptor (non-confidential mode).
///
/// Input: acceptor's SASL/GSSAPI response auth_bytes (32 bytes for 4-byte payload).
/// Output: payload bytes (typically 4 bytes: QOP + max_buffer).
pub fn gss_unwrap_wrap_token(
    etype: crypto::Etype,
    session_key: &[u8],
    token: &[u8],
) -> Result<(Vec<u8>, u64)> {
    // 最少: 16 header + 0 payload + checksum
    let min_len = 16 + etype.mac_len();
    if token.len() < min_len || token[0] != 0x05 || token[1] != 0x04 {
        return Err(KerberosError::Gss(format!(
            "bad WRAP token: len={}, tok_id=0x{:02x}{:02x}",
            token.len(),
            token[0],
            token[1]
        )));
    }

    // 解析 16-byte header
    let flags = token[2];
    let filler = token[3];
    let ec = u16::from_be_bytes([token[4], token[5]]) as usize; // checksum size
    let rrc = u16::from_be_bytes([token[6], token[7]]) as usize;
    let seqnum = u64::from_be_bytes([
        token[8], token[9], token[10], token[11], token[12], token[13], token[14], token[15],
    ]);

    if filler != 0xFF {
        return Err(KerberosError::Gss(format!(
            "bad WRAP filler: 0x{filler:02x}"
        )));
    }

    // 非保密性 WRAP: body = payload || checksum
    // 保密性 WRAP: body = encrypted(confounder || payload || header)
    let is_conf = (flags & 0x02) != 0;

    if is_conf {
        // —— 保密性 WRAP: 需要解密 ——
        // 解密后明文结构: confounder(16) || payload || [checksum(EC)] || embedded_header(16)
        // 保密性模式下 EC=0，所以没有 checksum 部分
        let ctext = &token[16..];

        // 先 undo RRC rotation (左旋转 payload 部分)
        let mut rotated = ctext.to_vec();
        if rrc != 0 {
            // rotate left the payload portion
            let plen = rotated.len();
            if plen > 0 {
                rotated = rotate_buf_left(&rotated, rrc % (plen * 8));
            }
        }

        // 解密: plaintext = AES-CTS decrypt(Kc_enc, iv=0, ciphertext)
        // Kc_enc = DK(session_key, ACCEPTOR_SEAL | 0xAA)
        let kc_enc = crypto::dk_for(
            etype,
            session_key,
            &crypto::usage_constant(GSS_USAGE_ACCEPTOR_SEAL, 0xAA),
        );
        let iv = vec![0u8; 16];
        let plain = crypto::cts_decrypt_for(etype, &kc_enc, &iv, &rotated);

        // 验证 embedded header (last 16 bytes of plaintext)
        if plain.len() < 32 {
            // 至少需要 confounder(16) + embedded_header(16)
            return Err(KerberosError::Gss("decrypted WRAP too short".into()));
        }
        let hdr_start = plain.len() - 16;
        let emb_hdr = &plain[hdr_start..];

        // embedded header: TOK_ID(2) | flags(1) | filler(1) | EC(2) | RRC(2) | seqnum(8)
        if emb_hdr.len() < 16
            || u16::from_be_bytes([emb_hdr[0], emb_hdr[1]]) != 0x0504
            || emb_hdr[2] != flags
            || emb_hdr[3] != 0xFF
        {
            return Err(KerberosError::Gss("WRAP embedded header mismatch".into()));
        }
        let emb_ec = u16::from_be_bytes([emb_hdr[4], emb_hdr[5]]) as usize;
        // payload 从 confounder(16) 之后开始，到 embedded_header 之前结束
        // 结构: confounder(16) || payload || [checksum(emb_ec)] || embedded_header(16)
        let payload_start = 16; // 跳过 confounder
        if emb_ec > hdr_start {
            return Err(KerberosError::Gss(
                "WRAP checksum size exceeds available space".into(),
            ));
        }
        let payload_end = hdr_start - emb_ec; // 跳过 checksum (如果有)
        if payload_end < payload_start {
            return Err(KerberosError::Gss("WRAP payload length underflow".into()));
        }
        let payload = plain[payload_start..payload_end].to_vec();
        let emb_seqnum = u64::from_be_bytes([
            emb_hdr[8],
            emb_hdr[9],
            emb_hdr[10],
            emb_hdr[11],
            emb_hdr[12],
            emb_hdr[13],
            emb_hdr[14],
            emb_hdr[15],
        ]);

        Ok((payload, emb_seqnum))
    } else {
        // —— 非保密性 WRAP: payload || checksum ——
        if ec > token.len() - 16 {
            return Err(KerberosError::Gss(format!(
                "WRAP checksum size {ec} > body len {}",
                token.len() - 16
            )));
        }

        let payload_len = token.len() - 16 - ec;
        let payload = token[16..16 + payload_len].to_vec();
        let received_cksum = &token[16 + payload_len..];

        // 验证 checksum
        // Kc = DK(session_key, ACCEPTOR_SEAL | 0x99) for HMAC checksum key per RFC 3961
        let kc = crypto::dk_for(
            etype,
            session_key,
            &crypto::usage_constant(GSS_USAGE_ACCEPTOR_SEAL, 0x99),
        );

        // checksum input = payload || header_with_EC=0_RRC=0
        let mut cksum_input = Vec::with_capacity(payload_len + 16);
        cksum_input.extend_from_slice(&payload);
        cksum_input.extend_from_slice(&token[0..4]); // TOK_ID(2) | flags(1) | filler(1)
        cksum_input.extend_from_slice(&[0, 0, 0, 0]); // EC=0, RRC=0
        cksum_input.extend_from_slice(&token[8..16]); // seqnum

        let computed_hmac = crypto::hmac_for(etype, &kc, &cksum_input);
        let computed_cksum = &computed_hmac[..ec];

        if computed_cksum != received_cksum {
            return Err(KerberosError::Gss(format!(
                "WRAP checksum mismatch: expected {:02x?}, got {:02x?}",
                &computed_cksum[..8],
                &received_cksum[..std::cmp::min(8, received_cksum.len())]
            )));
        }

        Ok((payload, seqnum))
    }
}

/// 构造客户端 RFC 4121 GSS_WRAP token（非保密性模式）。
///
/// 客户端是 initiator，使用 INITIATOR_SEAL | 0x99 派生 checksum key。
/// flags=0x00 (非 acceptor，非保密性)。
pub fn gss_wrap_token(
    etype: crypto::Etype,
    session_key: &[u8],
    qop: u8,
    max_buffer: u32,
    seq_num: u64,
) -> Result<Vec<u8>> {
    // Payload: [QOP(1), max_buf(3 bytes big-endian)]
    let payload = vec![
        qop,
        (max_buffer >> 16) as u8,
        (max_buffer >> 8) as u8,
        max_buffer as u8,
    ];

    // Kc = DK(session_key, INITIATOR_SEAL | 0x99) for checksum key
    let kc = crypto::dk_for(
        etype,
        session_key,
        &crypto::usage_constant(GSS_USAGE_INITIATOR_SEAL, 0x99),
    );

    let ec = etype.mac_len(); // 12 for SHA1-96, 24 for SHA384-192

    // 构建 checksum header (EC=0, RRC=0)
    let mut cksum_hdr = Vec::with_capacity(16);
    cksum_hdr.extend_from_slice(&[0x05, 0x04]);
    cksum_hdr.push(0x00); // flags: initiator, non-confidential
    cksum_hdr.push(0xFF); // filler
    cksum_hdr.extend_from_slice(&[0, 0, 0, 0]); // EC=0, RRC=0
    cksum_hdr.extend_from_slice(&seq_num.to_be_bytes());

    // checksum = HMAC(Kc, payload || header_with_EC=0)[0..ec]
    let mut cksum_input = Vec::new();
    cksum_input.extend_from_slice(&payload);
    cksum_input.extend_from_slice(&cksum_hdr);
    let hmac_full = crypto::hmac_for(etype, &kc, &cksum_input);
    let cksum = &hmac_full[..ec];

    // 构建完整 token: header || payload || checksum
    let mut token = Vec::with_capacity(16 + payload.len() + ec);
    // header
    token.extend_from_slice(&[0x05, 0x04]); // TOK_ID
    token.push(0x00); // flags: initiator
    token.push(0xFF); // filler
    token.extend_from_slice(&(ec as u16).to_be_bytes()); // EC = cksumsize
    token.extend_from_slice(&[0, 0]); // RRC = 0
    token.extend_from_slice(&seq_num.to_be_bytes()); // sequence number
    // body: payload || checksum
    token.extend_from_slice(&payload);
    token.extend_from_slice(cksum);

    Ok(token)
}

/// 缓冲区左旋转 nbits 位 (RFC 4121 RRC).
fn rotate_buf_left(data: &[u8], nbits: usize) -> Vec<u8> {
    let len = data.len();
    if len == 0 || nbits % (len * 8) == 0 {
        return data.to_vec();
    }
    let nbits = nbits % (len * 8);
    let nbytes = nbits / 8;
    let remain = nbits % 8;
    let mut result = vec![0u8; len];
    for i in 0..len {
        let src_idx = (i + nbytes) % len;
        let next_idx = (src_idx + 1) % len;
        let hi = data[src_idx] << remain;
        let lo = if remain == 0 {
            0
        } else {
            data[next_idx] >> (8 - remain)
        };
        result[i] = hi | lo;
    }
    result
}

/// krb5 机制 OID：1.2.840.113554.1.2.2。
pub const KRB5_OID: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x12, 0x01, 0x02, 0x02];

/// 将 Kerberos 令牌 (AP-REQ) 封装为 GSS InitialContextToken 格式：
///
/// ```text
/// GSS InitialContextToken ::= [APPLICATION 0] IMPLICIT SEQUENCE {
///     thisMech           MechType,           -- krb5 OID (11 bytes TLV)
///     innerContextToken  [INNER TOKEN FORMAT]
/// }
/// ```
///
/// MIT krb5 的 inner token 格式包含 RFC 1964 的 TOKEN_ID：
///
/// ```text
/// [APPLICATION 0] { OID, 0x01 0x00, AP-REQ DER }   -- 发起方
/// [APPLICATION 0] { OID, 0x02 0x00, AP-REP DER }   -- 接受方
/// ```
///
/// 兼容 MIT krb5 / Java GSS-API 的 acceptSecContext() 处理流程，
/// 要求 token 必须包含 OID + TOKEN_ID + AP-REQ 的完整封装。
pub fn wrap_gss_initial_token(inner_der: &[u8]) -> Vec<u8> {
    let oid_der = tlv(0x06, KRB5_OID); // OID tag + value = 11 bytes
    // MIT krb5 inner token: TOKEN_ID(0x01 0x00) + kerberos token
    let tok_id: &[u8] = &[0x01, 0x00]; // TOKEN_ID_INITIATOR (AP-REQ)
    let content_len = oid_der.len() + 2 + inner_der.len();
    let mut out = vec![0x60]; // [APPLICATION 0]
    out.extend(der_len(content_len));
    out.extend_from_slice(&oid_der);
    out.extend_from_slice(tok_id);
    out.extend_from_slice(inner_der);
    out
}

/// 从 GSS InitialContextToken 中剥离 OID + TOKEN_ID 包装，返回内层 Kerberos 令牌。
///
/// 输入格式: `[APPLICATION 0] { OID(11 bytes), TOKEN_ID(2 bytes), inner_token }`
/// - 发起方: TOKEN_ID = 0x0100 → 返回 AP-REQ DER
/// - 接受方: TOKEN_ID = 0x0200 → 返回 AP-REP DER
pub fn unwrap_gss_token(token: &[u8]) -> Result<Vec<u8>> {
    if token.len() < 2 {
        return Err(KerberosError::Gss("token too short for GSS wrapper".into()));
    }
    if token[0] != 0x60 {
        return Err(KerberosError::Gss(format!(
            "expected GSS InitialContextToken [APPLICATION 0] (0x60), got 0x{:02x}",
            token[0]
        )));
    }
    let (content_len, len_bytes) = read_len(&token[1..])?;
    let content_start = 1 + len_bytes;
    if token.len() < content_start + content_len {
        return Err(KerberosError::Gss(
            "truncated GSS InitialContextToken".into(),
        ));
    }
    let content = &token[content_start..content_start + content_len];
    // 跳过 OID: tag=0x06, len=0x09, value=9 字节 = 共 11 bytes
    let oid_tlv_len = 11; // 0x06 + 0x09 + 9 bytes
    if content.len() < oid_tlv_len + 2 || content[0] != 0x06 || content[1] != 0x09 {
        return Err(KerberosError::Gss("expected krb5 OID in GSS token".into()));
    }
    if &content[2..11] != KRB5_OID {
        return Err(KerberosError::Gss("unexpected OID in GSS token".into()));
    }
    // 跳过 TOKEN_ID: 2 bytes (0x01 0x00 or 0x02 0x00)
    Ok(content[oid_tlv_len + 2..].to_vec())
}

// ===========================================================================
// AP-REQ / AP-REP (RFC 4120 §5.5)
// ===========================================================================

/// AP-REQ 构建参数（来自 TGS-REP 的票据 + 会话密钥）。
pub struct ApReqOptions<'a> {
    pub cname: &'a PrincipalName,
    pub crealm: &'a str,
    /// 已编码的 Ticket DER。
    pub ticket_der: &'a [u8],
    pub session_etype: Etype,
    pub session_key: &'a [u8],
    pub ctime: &'a str,
    pub cusec: i32,
    pub seq_number: Option<u32>,
    pub subkey: Option<EncryptionKey>,
}

/// 构建 AP-REQ 并封装为 GSS InitialContextToken。
///
/// 包含 RFC 4121 §4.1 要求的 GSS channel binding checksum（空 binding）。
pub fn build_ap_req_token(opts: &ApReqOptions) -> Result<Vec<u8>> {
    // GSS channel binding checksum (type 0x8003 = CKSUMTYPE_KG_CB):
    //
    // MIT krb5 的 make_gss_checksum() 构造的 checksum 值格式（init_sec_context.c:330）:
    //
    //   [4 bytes LE]:  channel binding digest length (= 16 for MD5)
    //   [16 bytes]:     channel binding digest (16 zero bytes = GSS_C_NO_CHANNEL_BINDINGS)
    //   [4 bytes LE]:   GSS flags
    //
    // 接受端 process_checksum() 会验证:
    //   - checksum length >= 24 (MIN_8003_LEN)
    //   - cb_len == 16 (CB_MD5_LEN)
    //   - channel binding data 匹配
    //
    // Java acceptor (MIT krb5 JNI) 通过此处理函数校验。
    let cksumtype_val: i32 = 0x8003;
    // GSS flags: MUTUAL | REPLAY | SEQUENCE | INTEG
    // MIT krb5 GSS-API flag values (gssapi.h)：
    //   GSS_C_DELEG_FLAG   = 1   (bit 0)  - 我们不设置, 不委托
    //   GSS_C_MUTUAL_FLAG  = 2   (bit 1)  - ✓ 需要双向认证
    //   GSS_C_REPLAY_FLAG  = 4   (bit 2)  - ✓ 防重放
    //   GSS_C_SEQUENCE_FLAG= 8   (bit 3)  - ✓ 序列号保护
    //   GSS_C_CONF_FLAG    = 16  (bit 4)  - 不需要(明文)
    //   GSS_C_INTEG_FLAG   = 32  (bit 5)  - ✓ 完整性
    let gss_flags: u32 = 2 | 4 | 8 | 32; // = 0x002E
    // Java JDK 的 OverloadedChecksum 使用相同 flag 定义,
    // DELEG_FLAG=1, 若 bit0=1 则会尝试解析 KRB_CRED 委派扩展.
    let cksum_value = {
        let mut v = Vec::with_capacity(24);
        // cb_len = 16 (CB_MD5_LEN), little-endian
        v.extend_from_slice(&16u32.to_le_bytes());
        // null channel binding: 16 zero bytes (MD5 hash of no input)
        v.extend_from_slice(&[0u8; 16]);
        // GSS flags, little-endian
        v.extend_from_slice(&gss_flags.to_le_bytes());
        v
    };
    let cksum = {
        let mut inner = Vec::new();
        inner.extend(ctx_int(0, cksumtype_val));
        inner.extend(tlv_ctx(1, &tlv(0x04, &cksum_value)));
        tlv_seq(&inner)
    };

    let auth = Authenticator {
        authenticator_vno: 5,
        crealm: opts.crealm.to_string(),
        cname: opts.cname.clone(),
        cksum: Some(cksum),
        cusec: opts.cusec,
        ctime: opts.ctime.to_string(),
        subkey: opts.subkey.clone(),
        seq_number: opts.seq_number,
    };
    let auth_der = encode_authenticator(&auth);
    // 使用 MIT krb5 兼容加密模式 (HMAC over plaintext, 而非 over ciphertext).
    //
    // Java acceptor 通过 MIT krb5 JNI 调用 krb5int_dk_decrypt() 解密 Authenticator,
    // 其流程为:
    //   1. Decrypt → plaintext (= confounder || auth_der)
    //   2. HMAC(Ki, plaintext) → compare with received MAC
    //
    // 所以发送时必须使用 HMAC over plaintext 的 encrypt(), 而非
    // encrypt_rfc3962() (HMAC over ciphertext, 用于非 JNI 场景).
    let enc_auth_cipher = crypto::encrypt(
        opts.session_etype,
        opts.session_key,
        KEY_USAGE_AP_REQ_AUTH,
        &auth_der,
    )?;
    let enc_auth = EncryptedData {
        etype: opts.session_etype as i32,
        kvno: None,
        cipher: enc_auth_cipher,
    };
    let ap_req_der = encode_ap_req(opts.ticket_der, &enc_auth);
    Ok(wrap_gss_initial_token(&ap_req_der))
}

/// 验证 acceptor 回送的 AP-REP GSS InitialContextToken。
pub fn verify_ap_rep_token(
    token: &[u8],
    expected_etype: Etype,
    decrypt_key: &[u8],
    expected_ctime: &str,
    expected_cusec: i32,
) -> Result<EncApRepPart> {
    // acceptor 的 AP-REP 也被封装在 GSS InitialContextToken 中:
    // [APPLICATION 0] { OID, AP-REP }
    let inner = unwrap_gss_token(token)?;
    let enc_part = decode_ap_rep(&inner)?;
    // Java acceptor 使用标准 RFC 3962 加密 (HMAC over plaintext),
    // 与 MIT krb5 库一致.
    let plain = crypto::decrypt(
        expected_etype,
        decrypt_key,
        KEY_USAGE_AP_REP_ENC_PART,
        &enc_part.cipher,
    )?;
    let part = decode_enc_ap_rep_part(&plain)?;
    if part.ctime != expected_ctime {
        return Err(KerberosError::Gss(format!(
            "AP-REP ctime mismatch: expected {expected_ctime}, got {}",
            part.ctime
        )));
    }
    if part.cusec != expected_cusec {
        return Err(KerberosError::Gss(format!(
            "AP-REP cusec mismatch: expected {expected_cusec}, got {}",
            part.cusec
        )));
    }
    Ok(part)
}

/// Extract AP-REQ from a GSS InitialContextToken, returning (Ticket, encrypted Authenticator).
pub fn unwrap_ap_req_token(token: &[u8]) -> Result<(Ticket, EncryptedData)> {
    let inner = unwrap_gss_token(token)?;
    decode_ap_req(&inner)
}

// ===========================================================================
// GSS-API 上下文 — SASL/GSSAPI server-first 多轮握手驱动 trait
// ===========================================================================

/// Kerberos GSS-API (krb5 mechanism) authentication context.
///
/// Designed for server-first multi-round SASL/GSSAPI services (e.g. Kafka, LDAP):
///
/// 1. [`initial_token`] — returns the first client token (wrapped AP-REQ GSS token)
/// 2. [`handle_challenge`] — processes the peer's challenge (AP-REP),
///    returns `None` when authentication is complete; caller sends an empty token to finish
/// 3. [`is_complete`] — checks whether the context is established
pub trait GssContext: Send {
    /// Returns the first GSS token to send to the acceptor (AP-REQ).
    fn initial_token(&mut self) -> Result<Vec<u8>>;

    /// Process the challenge token (AP-REP) returned by the acceptor.
    ///
    /// Returns `None` when the context is established; caller should send an empty token to finish.
    fn handle_challenge(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Whether the context has been fully established.
    fn is_complete(&self) -> bool;

    /// After successful authentication, returns the authorized principal (client principal).
    fn authorized_principal(&self) -> Option<&str> {
        None
    }

    /// Standard GSS-API `init_sec_context` style step interface (recommended for external use).
    ///
    /// Unifies [`initial_token`] / [`handle_challenge`] into a single loop:
    ///
    /// - First call with `None` returns `Ok(Some(token))` — the first token to send to the acceptor (AP-REQ);
    /// - Subsequent calls with `Some(challenge)` (acceptor's reply token) return the next token to send;
    /// - When the handshake completes, returns `Ok(None)` and [`is_complete`] becomes `true`.
    ///
    /// ```ignore
    /// let mut ctx = gss.context_for("kafka/broker.example.com").await?;
    /// let mut out = ctx.step(None)?.unwrap(); // AP-REQ
    /// while let Some(token) = ctx.step(Some(&send_recv(out))).await? { out = token; }
    /// ```
    fn step(&mut self, input: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        match input {
            None => Ok(Some(self.initial_token()?)),
            Some(challenge) => self.handle_challenge(challenge),
        }
    }
}

// ===========================================================================
// NativeGssContext
// ===========================================================================

enum GssState {
    Init,
    AwaitingReply { ctime: String, cusec: i32 },
    AwaitingWrap, // AP-REP confirmed, awaiting GSS_WRAP token
    Established,
}

/// Native GSS context implementation based on krb5-gss KDC tickets.
pub struct NativeGssContext {
    state: GssState,
    /// 来自 TGS-REP 的服务票据 DER。
    service_ticket: Vec<u8>,
    /// 来自 TGS-REP 的会话密钥。
    session_etype: Etype,
    session_key: Vec<u8>,
    /// 客户端 principal。
    cname: PrincipalName,
    crealm: String,
    /// 认证完成后设置的服务端协商出的授权主体。
    auth_principal: Option<String>,
    /// RFC 4121 发送序列号 (客户端 → acceptor).
    seq_send: u64,
    /// RFC 4121 接收序列号 (acceptor → 客户端). 用于严格序列号/防重放校验。
    seq_recv: u64,
}

impl NativeGssContext {
    /// Build a GSS context from an already-acquired KDC ticket + session key.
    pub fn from_acquired(ticket: AcquiredTicket) -> Self {
        Self {
            state: GssState::Init,
            service_ticket: ticket.ticket_der,
            session_etype: ticket.session_etype,
            session_key: ticket.session_key,
            cname: ticket.cname,
            crealm: ticket.crealm,
            auth_principal: None,
            seq_send: 1, // 序列号从 1 开始 (对应 Authenticator.seq_number)
            seq_recv: 0, // 0 表示尚未收到对端 WRAP, 首次以对端序号为基线
        }
    }

    /// Construct a client RFC 4121 GSS_WRAP token (non-confidential) for per-message protection after handshake.
    ///
    /// Corresponds to GSS-API `gss_wrap`: returns a WRAP token encapsulating QOP / max_buffer,
    /// to be verified by the acceptor (see [`gss_unwrap_wrap_token`]).
    pub fn wrap(&self, qop: u8, max_buffer: u32, seq_num: u64) -> Result<Vec<u8>> {
        gss_wrap_token(
            self.session_etype,
            &self.session_key,
            qop,
            max_buffer,
            seq_num,
        )
    }

    /// Unwrap an RFC 4121 GSS_WRAP token from the acceptor (non-confidential).
    ///
    /// Corresponds to GSS-API `gss_unwrap`: verifies the checksum and returns the plaintext payload (QOP + max_buffer).
    /// Also performs strict sequence number validation: the first received sequence number becomes the baseline, and subsequent numbers must be monotonically non-decreasing (replay/ordering protection).
    pub fn unwrap(&mut self, token: &[u8]) -> Result<Vec<u8>> {
        let (payload, seq) = gss_unwrap_wrap_token(self.session_etype, &self.session_key, token)?;
        if seq < self.seq_recv {
            return Err(KerberosError::Gss(format!(
                "GSS WRAP replay/ordering violation: got seq {seq}, expected >= {}",
                self.seq_recv
            )));
        }
        self.seq_recv = seq + 1;
        Ok(payload)
    }
}

impl GssContext for NativeGssContext {
    fn initial_token(&mut self) -> Result<Vec<u8>> {
        match &self.state {
            GssState::Init => { /* ok，继续 */ }
            _ => {
                return Err(KerberosError::Gss(
                    "initial_token called more than once or after establish".into(),
                ));
            }
        }

        let ctime = utc_now_generalized();
        let cusec = now_micros();

        let opts = ApReqOptions {
            cname: &self.cname,
            crealm: &self.crealm,
            ticket_der: &self.service_ticket,
            session_etype: self.session_etype,
            session_key: &self.session_key,
            ctime: &ctime,
            cusec,
            seq_number: Some(1),
            subkey: None,
        };
        let token = build_ap_req_token(&opts)?;

        self.state = GssState::AwaitingReply { ctime, cusec };
        Ok(token)
    }

    fn handle_challenge(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>> {
        match &self.state {
            GssState::AwaitingReply { ctime, cusec } => {
                // ── 第 2 轮: 收到 AP-REP (GSS InitialContextToken, 0x60...) ──
                let ctime = ctime.clone();
                let cusec = *cusec;

                // 最佳努力验证 AP-REP (双向认证). 部分 acceptor 以非标准方式封装,
                // 验证失败时不阻塞握手, 继续进入 WRAP 阶段.
                // 验证通过且 acceptor 协商了 subkey 时, 后续 WRAP 必须改用 subkey 作为会话密钥.
                if !challenge.is_empty() {
                    match verify_ap_rep_token(
                        challenge,
                        self.session_etype,
                        &self.session_key,
                        &ctime,
                        cusec,
                    ) {
                        Ok(part) => {
                            if let Some(sk) = part.subkey {
                                self.session_etype = Etype::from_u32(sk.keytype as u32)
                                    .unwrap_or(self.session_etype);
                                self.session_key = sk.keyvalue;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "AP-REP verification failed (non-fatal, continuing handshake): {}",
                                e
                            );
                        }
                    }
                }

                self.state = GssState::AwaitingWrap;
                // 返回空 token → 第 3 轮: acceptor 发回 GSS_WRAP token
                Ok(Some(Vec::new()))
            }
            GssState::AwaitingWrap => {
                // ── 第 3 轮: 收到 GSS_WRAP token (32 bytes, 0x0504...) ──
                if challenge.len() < 2 || challenge[0] != 0x05 || challenge[1] != 0x04 {
                    return Err(KerberosError::Gss(format!(
                        "expected GSS_WRAP token (0x0504), got 0x{:02x}{:02x}",
                        challenge.first().copied().unwrap_or(0),
                        challenge.get(1).copied().unwrap_or(0),
                    )));
                }

                // 解包 GSS_WRAP: 提取 QOP 和 max buffer
                let (payload, _seq) =
                    gss_unwrap_wrap_token(self.session_etype, &self.session_key, challenge)?;

                let server_qop = payload[0];
                let max_buf =
                    ((payload[1] as u32) << 16) | ((payload[2] as u32) << 8) | (payload[3] as u32);

                // 构造客户端 WRAP token (使用 acceptor 协商的 QOP 和合理的 buffer size)
                let client_qop = server_qop;
                let client_max_buf = max_buf.min(0x100000); // 限制为 1MB
                let seq = self.seq_send;
                self.seq_send += 1;

                let client_wrap = gss_wrap_token(
                    self.session_etype,
                    &self.session_key,
                    client_qop,
                    client_max_buf,
                    seq,
                )?;

                // 认证完成
                self.auth_principal = Some(format!(
                    "{}@{}",
                    self.cname.name_string.join("/"),
                    self.crealm
                ));
                self.state = GssState::Established;

                Ok(Some(client_wrap))
            }
            GssState::Init => Err(KerberosError::Gss(
                "handle_challenge called before initial_token".into(),
            )),
            GssState::Established => Err(KerberosError::Gss(
                "handle_challenge called after context already established".into(),
            )),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GssState::Established)
    }

    fn authorized_principal(&self) -> Option<&str> {
        self.auth_principal.as_deref()
    }
}

// ===========================================================================
// 工厂函数
// ===========================================================================

/// Asynchronously create a GSS context: first acquire a service ticket from the KDC via `transport`, then build a [`NativeGssContext`].
///
/// `service` is in `service/hostname` format, e.g. `kafka/broker.example.com`
/// or `host/server.example.com`. Any service that interoperates with the krb5 GSS mechanism can be used.
pub async fn acquire_gss_context(
    creds: &KerberosCredentials,
    transport: &dyn KdcTransport,
    service: &str,
) -> Result<Box<dyn GssContext>> {
    let client = KerberosClient::new(creds).map_err(|e| {
        KerberosError::InvalidCredential(format!("create KerberosClient failed: {e}"))
    })?;
    let ticket = client
        .acquire_service_ticket(transport, service)
        .await
        .map_err(|e| {
            KerberosError::Protocol(format!("acquire_service_ticket('{service}') failed: {e}"))
        })?;
    Ok(Box::new(NativeGssContext::from_acquired(ticket)))
}

// ===========================================================================
// GssClient — high-level facade: combines KDC ticket acquisition + GSS context for one-stop integration
// ===========================================================================

/// High-level GSS client: combines KDC ticket acquisition + GSS context, providing **one-stop GSSAPI integration**.
///
/// Given credentials + KDC transport, call [`GssClient::context_for`] to obtain a
/// [`NativeGssContext`] that has already acquired a ticket from the KDC. The caller then uses
/// the standard GSS loop (`GssContext::step`) to complete the handshake with the acceptor.
/// This decouples the GSS layer from the specific transport (Kafka SASL / LDAP / raw TCP):
///
/// ```no_run
/// # async fn run() -> krb5_gss::Result<()> {
/// use krb5_gss::{KerberosCredentials, GssClient, gss::GssContext};
/// let creds = KerberosCredentials::new("client@EXAMPLE.COM").with_keytab("/path/krb5.keytab");
/// let gss = GssClient::new(&creds, "kdc.example.com", 88)?;
/// let mut ctx = gss.context_for("kafka/broker.example.com").await?;
/// let mut out = ctx.step(None)?.unwrap();        // AP-REQ
/// // out = send_to_acceptor_and_recv(out);        // application-layer transport (SASL frames, etc.)
/// // while let Some(tok) = ctx.step(Some(&resp))? { out = tok; /* send again */ }
/// # let _ = out; Ok(())
/// # }
/// ```
pub struct GssClient {
    krb5: Krb5Client,
}

impl GssClient {
    /// Create with a custom KDC transport (does not require tokio).
    pub fn with_transport(
        creds: &KerberosCredentials,
        transport: Box<dyn KdcTransport>,
    ) -> Result<Self> {
        Ok(Self {
            krb5: Krb5Client::with_transport(creds, transport)?,
        })
    }

    /// Create with the built-in tokio TCP KDC transport.
    pub fn new(creds: &KerberosCredentials, kdc_host: &str, kdc_port: u16) -> Result<Self> {
        Ok(Self {
            krb5: Krb5Client::new(creds, kdc_host, kdc_port)?,
        })
    }

    /// Obtain a GSS context for the specified service (KDC ticket already acquired, ready for acceptor handshake).
    ///
    /// The returned [`NativeGssContext`] can be driven directly with [`GssContext::step`] for the handshake;
    /// after the handshake, use [`NativeGssContext::wrap`] / [`NativeGssContext::unwrap`] for per-message protection.
    pub async fn context_for(&self, service: &str) -> Result<NativeGssContext> {
        let ticket = self.krb5.acquire_service_ticket(service).await?;
        Ok(NativeGssContext::from_acquired(ticket))
    }
}

// Time helper functions have been moved to the kerberos::util module

// ===========================================================================
// 测试
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kerberos::asn1::{decode_authenticator, encode_enc_data, encode_ticket, tlv_app};

    fn fake_ticket() -> Vec<u8> {
        encode_ticket(&Ticket {
            tkt_vno: 5,
            realm: "EXAMPLE.COM".into(),
            sname: PrincipalName {
                name_type: 2,
                name_string: vec!["kafka".to_string(), "broker.example.com".to_string()],
            },
            enc_part: EncryptedData {
                etype: 18,
                kvno: None,
                cipher: vec![0x11; 16],
            },
        })
    }

    fn fake_session() -> (Etype, Vec<u8>) {
        (Etype::Aes256CtsHmacSha196, vec![0x42u8; 32])
    }

    #[test]
    fn gss_token_roundtrip() {
        let inner = vec![0x6Eu8, 0x03, 0xAA, 0xBB, 0xCC];
        let tok = wrap_gss_initial_token(&inner);
        // 验证 GSS [APPLICATION 0] 前缀
        assert_eq!(tok[0], 0x60);
        // 验证 OID TLV + TOKEN_ID 完整存在
        // 格式: ... OID(11 bytes) TOKEN_ID(0x0100) inner
        let oid_tlv: Vec<u8> = {
            let mut v = vec![0x06, 0x09];
            v.extend_from_slice(KRB5_OID);
            v
        };
        let token_id_offset = tok.len() - inner.len() - 2;
        assert_eq!(
            &tok[token_id_offset..token_id_offset + 2],
            &[0x01, 0x00],
            "TOKEN_ID should be 0x0100"
        );
        assert_eq!(
            &tok[token_id_offset - 11..token_id_offset],
            &oid_tlv[..],
            "OID TLV should precede TOKEN_ID"
        );
        let out = unwrap_gss_token(&tok).unwrap();
        assert_eq!(out, inner);
    }

    #[test]
    fn ap_req_build_and_unwrap() {
        let ticket = fake_ticket();
        let (etype, key) = fake_session();
        let cname = PrincipalName {
            name_type: 1,
            name_string: vec!["client".to_string()],
        };
        let ctime = "20260713".to_string() + "101112Z";
        let opts = ApReqOptions {
            cname: &cname,
            crealm: "EXAMPLE.COM",
            ticket_der: &ticket,
            session_etype: etype,
            session_key: &key,
            ctime: &ctime,
            cusec: 654321,
            seq_number: Some(7),
            subkey: None,
        };
        let token = build_ap_req_token(&opts).unwrap();
        // GSS InitialContextToken 格式: [APPLICATION 0] (0x60) 开头
        assert_eq!(
            token[0], 0x60,
            "token should start with GSS [APPLICATION 0]"
        );
        let (dec_ticket, enc_auth) = unwrap_ap_req_token(&token).unwrap();
        assert_eq!(dec_ticket.realm, "EXAMPLE.COM");
        assert_eq!(enc_auth.etype, 18);
        let plain = crypto::decrypt(etype, &key, KEY_USAGE_AP_REQ_AUTH, &enc_auth.cipher).unwrap();
        let auth = decode_authenticator(&plain).unwrap();
        assert_eq!(auth.cname.name_string, vec!["client".to_string()]);
        assert_eq!(auth.ctime, ctime);
        assert_eq!(auth.cusec, 654321);
        assert_eq!(auth.seq_number, Some(7));
        // 验证 checksum 存在 (GSS channel binding)
        assert!(
            auth.cksum.is_some(),
            "authenticator should have GSS checksum"
        );
    }

    /// 模拟 acceptor AP-REP (GSS InitialContextToken 格式：含 OID 包装)。
    fn simulate_server_ap_rep(etype: Etype, key: &[u8], ctime: &str, cusec: i32) -> Vec<u8> {
        let mut part = Vec::new();
        part.extend(tlv_ctx(0, &tlv_gt(ctime)));
        part.extend(ctx_int(1, cusec));
        let enc_part_plain = tlv_seq(&part);
        let enc_cipher =
            crypto::encrypt(etype, key, KEY_USAGE_AP_REP_ENC_PART, &enc_part_plain).unwrap();
        let mut ap_rep = Vec::new();
        ap_rep.extend(ctx_int(0, 5));
        ap_rep.extend(ctx_int(1, 15));
        ap_rep.extend(tlv_ctx(
            2,
            &encode_enc_data(&EncryptedData {
                etype: etype as i32,
                kvno: None,
                cipher: enc_cipher,
            }),
        ));
        let ap_rep_der = tlv_app(15, &tlv_seq(&ap_rep));
        // 包装为 GSS InitialContextToken: [APPLICATION 0] { OID, AP-REP }
        wrap_gss_initial_token(&ap_rep_der)
    }

    fn tlv_gt(s: &str) -> Vec<u8> {
        let mut v = vec![0x18];
        v.extend(der_len(s.len()));
        v.extend_from_slice(s.as_bytes());
        v
    }

    #[test]
    fn ap_rep_verify_roundtrip() {
        let (etype, key) = fake_session();
        let ctime = "20260713".to_string() + "101112Z";
        let cusec = 654321;
        let server_token = simulate_server_ap_rep(etype, &key, &ctime, cusec);
        verify_ap_rep_token(&server_token, etype, &key, &ctime, cusec).unwrap();
        assert!(verify_ap_rep_token(&server_token, etype, &key, "20260713", cusec).is_err());
    }

    /// 验证新的 `initial_token` / `handle_challenge` API 的 mock 上下文。
    struct MockGssContext {
        initial_called: bool,
        rounds: usize,
        max: usize,
    }

    impl GssContext for MockGssContext {
        fn initial_token(&mut self) -> Result<Vec<u8>> {
            self.initial_called = true;
            Ok(vec![]) // 空 token，仅用于验证流程
        }

        fn handle_challenge(&mut self, _challenge: &[u8]) -> Result<Option<Vec<u8>>> {
            assert!(self.initial_called, "handle_challenge before initial_token");
            self.rounds += 1;
            if self.rounds > self.max {
                Ok(None)
            } else {
                Ok(Some(vec![0xAA; 8]))
            }
        }

        fn is_complete(&self) -> bool {
            self.rounds > self.max
        }
    }

    #[test]
    fn gss_handshake_loop() {
        let mut ctx = MockGssContext {
            initial_called: false,
            rounds: 0,
            max: 0, // 单轮：initial → handle → 收尾
        };

        // 1. initial_token → send to acceptor
        let first = ctx.initial_token().unwrap();
        assert_eq!(first, vec![]);

        // 2. handle server challenge → None => context complete
        let result = ctx.handle_challenge(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        assert!(result.is_none());
        assert!(ctx.is_complete());
    }

    /// 端到端验证 NativeGssContext：完整 3 轮握手 initial_token → AP-REP → WRAP。
    #[test]
    fn native_gss_initial_and_handle() {
        let cname = PrincipalName {
            name_type: 1,
            name_string: vec!["client".into()],
        };
        let ticket = fake_ticket();
        let session_key = vec![0x42u8; 32];
        let etype = Etype::Aes256CtsHmacSha196;

        let acquired = AcquiredTicket {
            ticket_der: ticket,
            session_etype: etype,
            session_key: session_key.clone(),
            cname: cname.clone(),
            crealm: "EXAMPLE.COM".into(),
        };

        let mut ctx = NativeGssContext::from_acquired(acquired);

        // 1. initial_token → AP-REQ
        let token = ctx.initial_token().unwrap();
        // GSS InitialContextToken 格式: 以 [APPLICATION 0] (0x60) 开头
        assert_eq!(
            token[0], 0x60,
            "token should start with GSS [APPLICATION 0]"
        );
        assert!(!ctx.is_complete());

        // 2. 从 token 中提取 Authenticator 时间戳用于模拟 server AP-REP
        let (_ticket, enc_auth) = unwrap_ap_req_token(&token).unwrap();
        let plain =
            crypto::decrypt(etype, &session_key, KEY_USAGE_AP_REQ_AUTH, &enc_auth.cipher).unwrap();
        let auth = decode_authenticator(&plain).unwrap();
        let server_token = simulate_server_ap_rep(etype, &session_key, &auth.ctime, auth.cusec);

        // 3. handle_challenge(AP-REP) → Some(empty) → 表示需要发送空 token 进入 WRAP 轮
        let round2_resp = ctx.handle_challenge(&server_token).unwrap();
        assert!(
            round2_resp.is_some(),
            "round 2 should return empty token for WRAP exchange"
        );
        assert_eq!(
            round2_resp.unwrap(),
            Vec::<u8>::new(),
            "round 2 should return empty token"
        );
        assert!(!ctx.is_complete());

        // 4. 模拟 acceptor 的 GSS_WRAP token (non-confidential)
        //    构建 acceptor wrap token: [QOP=0, max_buf=0x100000]
        let acceptor_wrap = simulate_server_wrap(etype, &session_key);
        let round3_resp = ctx.handle_challenge(&acceptor_wrap).unwrap();
        assert!(
            round3_resp.is_some(),
            "round 3 should return client WRAP token"
        );
        assert!(
            !round3_resp.unwrap().is_empty(),
            "client WRAP should not be empty"
        );
        assert!(ctx.is_complete());
        assert_eq!(ctx.authorized_principal(), Some("client@EXAMPLE.COM"));
    }

    /// 模拟 acceptor 发送的 non-confidential GSS_WRAP token。
    fn simulate_server_wrap(etype: Etype, session_key: &[u8]) -> Vec<u8> {
        let payload = vec![0u8, 0x10, 0x00, 0x00]; // QOP=0, max_buf=1MB
        let ec = etype.mac_len(); // 12
        let seq_num: u64 = 1;
        let flags: u8 = 0x01; // FLAG_SENDER_IS_ACCEPTOR

        // Kc = DK(session_key, ACCEPTOR_SEAL | 0x99)
        let kc = crypto::dk_for(
            etype,
            session_key,
            &crypto::usage_constant(super::GSS_USAGE_ACCEPTOR_SEAL, 0x99),
        );

        // checksum input: payload || header_with_EC=0
        let mut cksum_input = Vec::new();
        cksum_input.extend_from_slice(&payload);
        cksum_input.extend_from_slice(&[0x05, 0x04]); // TOK_ID
        cksum_input.push(flags); // flags
        cksum_input.push(0xFF); // filler
        cksum_input.extend_from_slice(&[0, 0, 0, 0]); // EC=0, RRC=0
        cksum_input.extend_from_slice(&seq_num.to_be_bytes());

        let hmac = crypto::hmac_for(etype, &kc, &cksum_input);
        let cksum = &hmac[..ec];

        let mut token = Vec::with_capacity(16 + payload.len() + ec);
        token.extend_from_slice(&[0x05, 0x04]); // TOK_ID
        token.push(flags); //
        token.push(0xFF); //
        token.extend_from_slice(&(ec as u16).to_be_bytes()); // EC
        token.extend_from_slice(&[0, 0]); // RRC
        token.extend_from_slice(&seq_num.to_be_bytes()); // seqnum
        token.extend_from_slice(&payload);
        token.extend_from_slice(cksum);
        token
    }

    /// 模拟 acceptor 发送的 confidential GSS_WRAP token (RFC 4121 §4.2.2)。
    ///
    /// 保密性 WRAP 结构:
    ///   Header (16 bytes): TOK_ID | flags | filler | EC | RRC | seqnum
    ///   Body: AES-CTS-encrypt(confounder || payload || embedded_header)
    ///
    /// embedded_header = TOK_ID(2) | flags(1) | filler(1) | EC(2) | RRC(2) | seqnum(8)
    fn simulate_server_wrap_confidential(
        etype: Etype,
        session_key: &[u8],
        payload: &[u8],
        seq_num: u64,
    ) -> Vec<u8> {
        let flags: u8 = 0x03; // FLAG_SENDER_IS_ACCEPTOR | FLAG_WRAP_CONFIDENTIAL
        let ec: u16 = 0; // 保密性模式下 EC 字段为 0 (checksum 嵌入密文)
        let rrc: u16 = 0;

        // 生成 16 字节 confounder
        let confounder = vec![0xAAu8; 16];

        // 构建 embedded header (16 bytes)
        let mut emb_hdr = Vec::with_capacity(16);
        emb_hdr.extend_from_slice(&[0x05, 0x04]); // TOK_ID
        emb_hdr.push(flags);
        emb_hdr.push(0xFF); // filler
        emb_hdr.extend_from_slice(&(ec as u16).to_be_bytes()); // EC=0
        emb_hdr.extend_from_slice(&(rrc as u16).to_be_bytes()); // RRC=0
        emb_hdr.extend_from_slice(&seq_num.to_be_bytes());

        // 明文 = confounder || payload || embedded_header
        let mut plaintext = Vec::new();
        plaintext.extend_from_slice(&confounder);
        plaintext.extend_from_slice(payload);
        plaintext.extend_from_slice(&emb_hdr);

        // Kc_enc = DK(session_key, ACCEPTOR_SEAL | 0xAA)
        let kc_enc = crypto::dk_for(
            etype,
            session_key,
            &crypto::usage_constant(super::GSS_USAGE_ACCEPTOR_SEAL, 0xAA),
        );

        // AES-CTS 加密 (IV = 0)
        let iv = vec![0u8; 16];
        let ciphertext = crypto::cts_encrypt_for(etype, &kc_enc, &iv, &plaintext);

        // 构建完整 token: header || ciphertext
        let mut token = Vec::with_capacity(16 + ciphertext.len());
        token.extend_from_slice(&[0x05, 0x04]); // TOK_ID
        token.push(flags);
        token.push(0xFF); // filler
        token.extend_from_slice(&(ec as u16).to_be_bytes()); // EC=0
        token.extend_from_slice(&(rrc as u16).to_be_bytes()); // RRC=0
        token.extend_from_slice(&seq_num.to_be_bytes()); // seqnum
        token.extend_from_slice(&ciphertext);

        token
    }

    #[test]
    fn gss_token_unwrap_rejects_wrong_format() {
        // 旧格式 (TOKEN_ID) 应被拒绝
        let old_style = {
            let mut t = vec![0x01, 0x00]; // TOKEN_ID_INITIATOR
            t.extend_from_slice(&[0x6E, 0x00]);
            t
        };
        assert!(unwrap_gss_token(&old_style).is_err());
    }

    /// 验证标准 GSS 循环 (`step`) 驱动完整 3 轮握手, 与 `initial_token`/`handle_challenge` 等价。
    #[test]
    fn step_unified_handshake() {
        let cname = PrincipalName {
            name_type: 1,
            name_string: vec!["client".into()],
        };
        let ticket = fake_ticket();
        let session_key = vec![0x42u8; 32];
        let etype = Etype::Aes256CtsHmacSha196;

        let acquired = AcquiredTicket {
            ticket_der: ticket,
            session_etype: etype,
            session_key: session_key.clone(),
            cname: cname.clone(),
            crealm: "EXAMPLE.COM".into(),
        };

        let mut ctx = NativeGssContext::from_acquired(acquired);

        // 1. step(None) → 首个 AP-REQ token
        let token = ctx
            .step(None)
            .unwrap()
            .expect("first step must emit a token");
        assert_eq!(
            token[0], 0x60,
            "token should start with GSS [APPLICATION 0]"
        );

        // 2. 从 token 提取时间戳, 模拟 acceptor AP-REP
        let (_t, enc_auth) = unwrap_ap_req_token(&token).unwrap();
        let plain =
            crypto::decrypt(etype, &session_key, KEY_USAGE_AP_REQ_AUTH, &enc_auth.cipher).unwrap();
        let auth = decode_authenticator(&plain).unwrap();
        let server_token = simulate_server_ap_rep(etype, &session_key, &auth.ctime, auth.cusec);

        // 3. step(Some(AP-REP)) → 空 token (进入 WRAP 轮)
        let r2 = ctx
            .step(Some(&server_token))
            .unwrap()
            .expect("wrap round expected");
        assert!(r2.is_empty());

        // 4. step(Some(WRAP)) → 客户端 WRAP token, 上下文建立
        let acceptor_wrap = simulate_server_wrap(etype, &session_key);
        let r3 = ctx.step(Some(&acceptor_wrap)).unwrap();
        assert!(r3.is_some());
        assert!(ctx.is_complete());
        assert_eq!(ctx.authorized_principal(), Some("client@EXAMPLE.COM"));

        // per-message: 客户端 wrap 生成 INITIATOR_SEAL token (形状校验)
        let out = ctx.wrap(0, 0x100000, 1).unwrap();
        assert_eq!(&out[..2], &[0x05, 0x04]); // TOK_ID
        assert_eq!(out[2], 0x00); // flags: initiator, non-confidential

        // per-message: 客户端 unwrap 校验 acceptor 发来的 WRAP token (ACCEPTOR_SEAL)
        let acceptor_wrap = simulate_server_wrap(etype, &session_key);
        let payload = ctx.unwrap(&acceptor_wrap).unwrap();
        assert_eq!(&payload[..1], &[0]); // QOP=0
        assert_eq!(
            u32::from_be_bytes([0, payload[1], payload[2], payload[3]]),
            0x100000
        );
    }

    /// 验证保密性 WRAP token 的解包 (RFC 4121 §4.2.2)。
    ///
    /// 保密性模式下，payload 被 AES-CTS 加密，embedded header 嵌入密文末尾。
    #[test]
    fn confidential_wrap_unwrap_roundtrip() {
        let etype = Etype::Aes256CtsHmacSha196;
        let session_key = vec![0x42u8; 32];
        let payload = vec![0u8, 0x10, 0x00, 0x00]; // QOP=0, max_buf=1MB
        let seq_num: u64 = 42;

        // 模拟 acceptor 发送保密性 WRAP token
        let conf_wrap = simulate_server_wrap_confidential(etype, &session_key, &payload, seq_num);

        // 验证 token 格式
        assert_eq!(&conf_wrap[..2], &[0x05, 0x04], "TOK_ID should be 0x0504");
        assert_eq!(
            conf_wrap[2], 0x03,
            "flags should be ACCEPTOR | CONFIDENTIAL"
        );
        assert_eq!(conf_wrap[3], 0xFF, "filler should be 0xFF");

        // 解包保密性 WRAP token
        let (dec_payload, dec_seq) =
            gss_unwrap_wrap_token(etype, &session_key, &conf_wrap).unwrap();

        // 验证解包结果
        assert_eq!(dec_payload, payload, "payload should match");
        assert_eq!(dec_seq, seq_num, "sequence number should match");
    }

    /// 验证保密性 WRAP token 的序列号校验 (防重放)。
    #[test]
    fn confidential_wrap_replay_detection() {
        let etype = Etype::Aes256CtsHmacSha196;
        let session_key = vec![0x42u8; 32];
        let payload = vec![0u8, 0x10, 0x00, 0x00];

        let cname = PrincipalName {
            name_type: 1,
            name_string: vec!["client".into()],
        };
        let ticket = fake_ticket();
        let acquired = AcquiredTicket {
            ticket_der: ticket,
            session_etype: etype,
            session_key: session_key.clone(),
            cname: cname.clone(),
            crealm: "EXAMPLE.COM".into(),
        };

        let mut ctx = NativeGssContext::from_acquired(acquired);

        // 完成握手进入 Established 状态
        let token = ctx.initial_token().unwrap();
        let (_t, enc_auth) = unwrap_ap_req_token(&token).unwrap();
        let plain =
            crypto::decrypt(etype, &session_key, KEY_USAGE_AP_REQ_AUTH, &enc_auth.cipher).unwrap();
        let auth = decode_authenticator(&plain).unwrap();
        let server_token = simulate_server_ap_rep(etype, &session_key, &auth.ctime, auth.cusec);
        ctx.handle_challenge(&server_token).unwrap();
        let acceptor_wrap = simulate_server_wrap(etype, &session_key);
        ctx.handle_challenge(&acceptor_wrap).unwrap();
        assert!(ctx.is_complete());

        // 首次 unwrap 保密性 WRAP (seq=10)
        let wrap1 = simulate_server_wrap_confidential(etype, &session_key, &payload, 10);
        let result1 = ctx.unwrap(&wrap1);
        assert!(result1.is_ok(), "first unwrap should succeed");

        // 重放攻击: 再次发送 seq=10 应被拒绝
        let wrap_replay = simulate_server_wrap_confidential(etype, &session_key, &payload, 10);
        let result_replay = ctx.unwrap(&wrap_replay);
        assert!(
            result_replay.is_err(),
            "replay of seq=10 should be rejected"
        );

        // 乱序: seq=5 < 当前基线 11 应被拒绝
        let wrap_old = simulate_server_wrap_confidential(etype, &session_key, &payload, 5);
        let result_old = ctx.unwrap(&wrap_old);
        assert!(result_old.is_err(), "out-of-order seq=5 should be rejected");

        // 正常递增: seq=11 应成功
        let wrap2 = simulate_server_wrap_confidential(etype, &session_key, &payload, 11);
        let result2 = ctx.unwrap(&wrap2);
        assert!(result2.is_ok(), "seq=11 should succeed");
    }

    /// 验证保密性 WRAP token 的密钥错误检测。
    #[test]
    fn confidential_wrap_wrong_key_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let session_key = vec![0x42u8; 32];
        let wrong_key = vec![0x99u8; 32];
        let payload = vec![0u8, 0x10, 0x00, 0x00];

        // 用正确密钥加密
        let conf_wrap = simulate_server_wrap_confidential(etype, &session_key, &payload, 1);

        // 用错误密钥解密应失败 (embedded header 校验失败)
        let result = gss_unwrap_wrap_token(etype, &wrong_key, &conf_wrap);
        assert!(result.is_err(), "decryption with wrong key should fail");
    }

    /// 验证 AES-128 保密性 WRAP 也能正常工作。
    #[test]
    fn confidential_wrap_aes128_roundtrip() {
        let etype = Etype::Aes128CtsHmacSha196;
        let session_key = vec![0x11u8; 16]; // AES-128 密钥 16 字节
        let payload = vec![0u8, 0x08, 0x00, 0x00]; // QOP=0, max_buf=512KB
        let seq_num: u64 = 100;

        let conf_wrap = simulate_server_wrap_confidential(etype, &session_key, &payload, seq_num);
        let (dec_payload, dec_seq) =
            gss_unwrap_wrap_token(etype, &session_key, &conf_wrap).unwrap();

        assert_eq!(dec_payload, payload);
        assert_eq!(dec_seq, seq_num);
    }
}
