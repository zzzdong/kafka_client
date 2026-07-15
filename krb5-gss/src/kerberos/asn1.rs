//! Kerberos ASN.1 DER encoding/decoding (hand-written TLV, fully controllable and unit-testable).
//!
//! Kerberos messages make heavy use of explicit/implicit context tags. Rather than relying on
//! `der` crate derive macros, this module implements a minimal DER writer/reader directly, allowing
//! precise control over every tag byte, verified by roundtrip unit tests.
//!
//! Note: real-world interoperability requires byte-level comparison with real KDC messages
//! (no KDC available in this environment). The roundtrip tests below only prove that encoding
//! and decoding are inverse operations, not that the wire format is fully conformant.
use crate::error::{KerberosError, Result};

// ----------------------------- DER 基础 -----------------------------

/// 编码 DER 长度字段。
pub(crate) fn der_len(n: usize) -> Vec<u8> {
    if n < 0x80 {
        vec![n as u8]
    } else {
        let mut bytes = Vec::new();
        let mut v = n;
        while v > 0 {
            bytes.push((v & 0xff) as u8);
            v >>= 8;
        }
        bytes.reverse();
        let mut out = vec![0x80 | bytes.len() as u8];
        out.extend_from_slice(&bytes);
        out
    }
}

/// 构造一个 TLV (tag + length + content)。
pub fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut v = vec![tag];
    v.extend(der_len(content.len()));
    v.extend_from_slice(content);
    v
}

/// 构造显式 context tag `[n]` (constructed, 0xA0 | n)。
pub fn tlv_ctx(n: u8, content: &[u8]) -> Vec<u8> {
    tlv(0xA0 | n, content)
}

/// 在 context tag `[n]` 中包装一个 DER INTEGER (显式标签, 含 0x02 标记和长度)。
/// Kerberos ASN.1 模块使用 EXPLICIT TAGS, context tag 需包裹完整 INTEGER DER。
pub fn ctx_int(n: u8, v: i32) -> Vec<u8> {
    tlv_ctx(n, &tlv(0x02, &encode_int(v)))
}

/// 构造 SEQUENCE (0x30)。
pub fn tlv_seq(content: &[u8]) -> Vec<u8> {
    tlv(0x30, content)
}

/// 构造 `[APPLICATION n]` (constructed, 0x60 | n)。
pub fn tlv_app(n: u8, content: &[u8]) -> Vec<u8> {
    tlv(0x60 | n, content)
}

/// 从 `data` 头部取出一个 TLV, 返回 (tag, content, rest)。
fn take_tlv(data: &[u8]) -> Result<(u8, &[u8], &[u8])> {
    if data.is_empty() {
        return Err(KerberosError::Asn1("unexpected end of data".into()));
    }
    let tag = data[0];
    let (len, l) = read_len(&data[1..])?;
    if data.len() < 1 + l + len {
        return Err(KerberosError::Asn1("truncated TLV".into()));
    }
    let content = &data[1 + l..1 + l + len];
    let rest = &data[1 + l + len..];
    Ok((tag, content, rest))
}

/// 读取 DER 长度字段, 返回 (长度, 占用字节数)。
pub(crate) fn read_len(data: &[u8]) -> Result<(usize, usize)> {
    if data.is_empty() {
        return Err(KerberosError::Asn1("truncated length".into()));
    }
    let first = data[0];
    if first < 0x80 {
        Ok((first as usize, 1))
    } else {
        let nbytes = (first & 0x7f) as usize;
        if data.len() < 1 + nbytes {
            return Err(KerberosError::Asn1("truncated length".into()));
        }
        let mut len = 0usize;
        for i in 0..nbytes {
            len = (len << 8) | data[1 + i] as usize;
        }
        Ok((len, 1 + nbytes))
    }
}

/// 收集一段 SEQUENCE 内容里的所有顶层 TLV, 返回 (tag, content) 列表。
fn collect_tlvs(data: &[u8]) -> Result<Vec<(u8, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let (tag, content, r) = take_tlv(rest)?;
        out.push((tag, content.to_vec()));
        rest = r;
    }
    Ok(out)
}

fn field<'a>(fields: &'a [(u8, Vec<u8>)], tag: u8) -> Result<&'a [u8]> {
    fields
        .iter()
        .find(|(t, _)| *t == tag)
        .map(|(_, c)| c.as_slice())
        .ok_or_else(|| KerberosError::Asn1(format!("missing context field 0x{tag:02x}")))
}

pub(crate) fn encode_int(v: i32) -> Vec<u8> {
    if v >= 0 {
        let mut b = v.to_be_bytes().to_vec();
        while b.len() > 1 && b[0] == 0 {
            b.remove(0);
        }
        if b[0] & 0x80 != 0 {
            b.insert(0, 0);
        }
        b
    } else {
        let mut b = v.to_be_bytes().to_vec();
        while b.len() > 1 && b[0] == 0xff && (b[1] & 0x80 != 0) {
            b.remove(0);
        }
        b
    }
}

fn decode_int(content: &[u8]) -> Result<i64> {
    if content.is_empty() {
        return Err(KerberosError::Asn1("empty INTEGER".into()));
    }
    let mut v: i64 = if content[0] & 0x80 != 0 { -1 } else { 0 };
    for &b in content {
        v = (v << 8) | b as i64;
    }
    Ok(v)
}

/// 从 EXPLICIT 标签包裹的 context 字段中提取 INTEGER 值。
/// 输入为 context tag 的内容 (包含内层 0x02 tag + length + value)。
fn decode_ctx_int(data: &[u8]) -> Result<i64> {
    let (_, content, _) = take_tlv(data)?;
    decode_int(content)
}

// ----------------------------- 结构定义 -----------------------------

#[derive(Debug, Clone)]
pub struct PrincipalName {
    pub name_type: i32,
    pub name_string: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct EncryptedData {
    pub etype: i32,
    pub kvno: Option<i32>,
    pub cipher: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Ticket {
    pub tkt_vno: i32,
    pub realm: String,
    pub sname: PrincipalName,
    pub enc_part: EncryptedData,
}

#[derive(Debug, Clone)]
pub struct Authenticator {
    pub authenticator_vno: i32,
    pub crealm: String,
    pub cname: PrincipalName,
    pub cksum: Option<Vec<u8>>,
    pub cusec: i32,
    /// KerberosTime, 编码为 GeneralizedTime (格式 `YYYYMMDDHHMMSSZ`)。
    pub ctime: String,
    pub subkey: Option<EncryptionKey>,
    pub seq_number: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct EncryptionKey {
    pub keytype: i32,
    pub keyvalue: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct EncApRepPart {
    pub ctime: String,
    pub cusec: i32,
    pub subkey: Option<EncryptionKey>,
    pub seq_number: Option<u32>,
}

// ----------------------------- 编码 -----------------------------

pub fn encode_principal_name(pn: &PrincipalName) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(0, pn.name_type));
    let mut names = Vec::new();
    for s in &pn.name_string {
        names.extend(tlv(0x1B, s.as_bytes()));
    }
    inner.extend(tlv_ctx(1, &tlv(0x30, &names)));
    tlv(0x30, &inner)
}

pub fn encode_enc_data(e: &EncryptedData) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(0, e.etype));
    if let Some(kvno) = e.kvno {
        inner.extend(ctx_int(1, kvno));
    }
    inner.extend(tlv_ctx(2, &tlv(0x04, &e.cipher)));
    tlv(0x30, &inner)
}

pub fn encode_enc_key(k: &EncryptionKey) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(0, k.keytype));
    inner.extend(tlv_ctx(1, &tlv(0x04, &k.keyvalue)));
    tlv(0x30, &inner)
}

pub fn encode_ticket(t: &Ticket) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(0, t.tkt_vno));
    inner.extend(tlv_ctx(1, &tlv(0x1B, t.realm.as_bytes())));
    inner.extend(tlv_ctx(2, &encode_principal_name(&t.sname)));
    inner.extend(tlv_ctx(3, &encode_enc_data(&t.enc_part)));
    // [APPLICATION 1] SEQUENCE
    tlv(0x61, &tlv_seq(&inner))
}

pub fn encode_authenticator(a: &Authenticator) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(0, a.authenticator_vno));
    inner.extend(tlv_ctx(1, &tlv(0x1B, a.crealm.as_bytes())));
    inner.extend(tlv_ctx(2, &encode_principal_name(&a.cname)));
    if let Some(cksum) = &a.cksum {
        inner.extend(tlv_ctx(3, cksum));
    }
    inner.extend(ctx_int(4, a.cusec));
    inner.extend(tlv_ctx(5, &tlv(0x18, a.ctime.as_bytes())));
    if let Some(subkey) = &a.subkey {
        inner.extend(tlv_ctx(6, &encode_enc_key(subkey)));
    }
    if let Some(seq) = a.seq_number {
        inner.extend(ctx_int(7, seq as i32));
    }
    // [APPLICATION 2] SEQUENCE
    tlv(0x62, &tlv_seq(&inner))
}

/// 构建 AP-REQ DER (`[APPLICATION 14]`)。
///
/// `ticket_der` 为已编码的 Ticket (即 `encode_ticket` 产物, 以 `0x61` 开头),
/// `enc_authenticator` 为已加密的 Authenticator (`EncryptedData`)。
pub fn encode_ap_req(ticket_der: &[u8], enc_authenticator: &EncryptedData) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(0, 5)); // pvno
    inner.extend(ctx_int(1, 14)); // msg-type
    inner.extend(tlv_ctx(2, &tlv(0x03, &[0x00, 0x00, 0x00, 0x00, 0x00]))); // ap-options (32-bit, none)
    inner.extend(tlv_ctx(3, ticket_der)); // ticket [3]
    inner.extend(tlv_ctx(4, &encode_enc_data(enc_authenticator))); // authenticator [4]
    // [APPLICATION 14] SEQUENCE
    tlv(0x6E, &tlv_seq(&inner))
}

// ----------------------------- 解码 -----------------------------

pub fn decode_principal_name(data: &[u8]) -> Result<PrincipalName> {
    let (tag, content, _) = take_tlv(data)?;
    if tag != 0x30 {
        return Err(KerberosError::Asn1(format!(
            "expected PrincipalName SEQUENCE (0x30), got 0x{tag:02x}"
        )));
    }
    parse_principal_name(content)
}

fn parse_principal_name(content: &[u8]) -> Result<PrincipalName> {
    let fields = collect_tlvs(content)?;
    let name_type = decode_ctx_int(field(&fields, 0xA0)?)? as i32;
    let names_content = field(&fields, 0xA1)?;
    let (_t, names_seq, _) = take_tlv(names_content)?;
    let mut name_string = Vec::new();
    let mut rest = names_seq;
    while !rest.is_empty() {
        let (_tag, c, r) = take_tlv(rest)?;
        name_string.push(String::from_utf8_lossy(c).into_owned());
        rest = r;
    }
    Ok(PrincipalName {
        name_type,
        name_string,
    })
}

pub fn decode_enc_data(data: &[u8]) -> Result<EncryptedData> {
    let (tag, content, _) = take_tlv(data)?;
    if tag != 0x30 {
        return Err(KerberosError::Asn1(format!(
            "expected EncryptedData SEQUENCE (0x30), got 0x{tag:02x}"
        )));
    }
    parse_enc_data(content)
}

fn parse_enc_data(content: &[u8]) -> Result<EncryptedData> {
    let fields = collect_tlvs(content)?;
    let etype = decode_ctx_int(field(&fields, 0xA0)?)? as i32;
    let kvno = match field(&fields, 0xA1) {
        Ok(c) => Some(decode_ctx_int(c)? as i32),
        Err(_) => None,
    };
    let cipher_content = field(&fields, 0xA2)?;
    let (_t, cipher, _) = take_tlv(cipher_content)?;
    Ok(EncryptedData {
        etype,
        kvno,
        cipher: cipher.to_vec(),
    })
}

pub fn decode_ticket(content: &[u8]) -> Result<Ticket> {
    let (tag, c, _) = take_tlv(content)?;
    if tag != 0x61 {
        return Err(KerberosError::Asn1(format!(
            "expected Ticket [APPLICATION 1] (0x61), got 0x{tag:02x}"
        )));
    }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    let tkt_vno = decode_ctx_int(field(&fields, 0xA0)?)? as i32;
    let realm = {
        let rc = field(&fields, 0xA1)?;
        let (_t, s, _) = take_tlv(rc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let sname = decode_principal_name(field(&fields, 0xA2)?)?;
    let enc_part = decode_enc_data(field(&fields, 0xA3)?)?;
    Ok(Ticket {
        tkt_vno,
        realm,
        sname,
        enc_part,
    })
}

/// 从已编码的 Ticket DER 中提取服务 principal 名 (调试用)。
pub fn get_ticket_sname(ticket_der: &[u8]) -> Option<String> {
    if let Ok(t) = decode_ticket(ticket_der) {
        Some(t.sname.name_string.join("/"))
    } else {
        None
    }
}

pub fn decode_authenticator(content: &[u8]) -> Result<Authenticator> {
    let (tag, c, _) = take_tlv(content)?;
    if tag != 0x62 {
        return Err(KerberosError::Asn1(format!(
            "expected Authenticator [APPLICATION 2] (0x62), got 0x{tag:02x}"
        )));
    }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    let authenticator_vno = decode_ctx_int(field(&fields, 0xA0)?)? as i32;
    let crealm = {
        let rc = field(&fields, 0xA1)?;
        let (_t, s, _) = take_tlv(rc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let cname = decode_principal_name(field(&fields, 0xA2)?)?;
    let cksum = match field(&fields, 0xA3) {
        Ok(rc) => {
            let (_t, s, _) = take_tlv(rc)?;
            Some(s.to_vec())
        }
        Err(_) => None,
    };
    let cusec = decode_ctx_int(field(&fields, 0xA4)?)? as i32;
    let ctime = {
        let cc = field(&fields, 0xA5)?;
        let (_t, s, _) = take_tlv(cc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let subkey = match field(&fields, 0xA6) {
        Ok(c) => Some(decode_enc_key(c)?),
        Err(_) => None,
    };
    let seq_number = match field(&fields, 0xA7) {
        Ok(c) => Some(decode_ctx_int(c)? as u32),
        Err(_) => None,
    };
    Ok(Authenticator {
        authenticator_vno,
        crealm,
        cname,
        cksum,
        cusec,
        ctime,
        subkey,
        seq_number,
    })
}

pub fn decode_enc_key(data: &[u8]) -> Result<EncryptionKey> {
    let (tag, content, _) = take_tlv(data)?;
    if tag != 0x30 {
        return Err(KerberosError::Asn1(format!(
            "expected EncryptionKey SEQUENCE (0x30), got 0x{tag:02x}"
        )));
    }
    parse_enc_key(content)
}

fn parse_enc_key(content: &[u8]) -> Result<EncryptionKey> {
    let fields = collect_tlvs(content)?;
    let keytype = decode_ctx_int(field(&fields, 0xA0)?)? as i32;
    let kv = field(&fields, 0xA1)?;
    let (_t, keyvalue, _) = take_tlv(kv)?;
    Ok(EncryptionKey {
        keytype,
        keyvalue: keyvalue.to_vec(),
    })
}

pub fn decode_enc_ap_rep_part(data: &[u8]) -> Result<EncApRepPart> {
    let (tag, content, _) = take_tlv(data)?;
    if tag != 0x30 {
        return Err(KerberosError::Asn1(format!(
            "expected EncAPRepPart SEQUENCE (0x30), got 0x{tag:02x}"
        )));
    }
    parse_enc_ap_rep_part(content)
}

fn parse_enc_ap_rep_part(content: &[u8]) -> Result<EncApRepPart> {
    let fields = collect_tlvs(content)?;
    let ctime = {
        let cc = field(&fields, 0xA0)?;
        let (_t, s, _) = take_tlv(cc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let cusec = decode_ctx_int(field(&fields, 0xA1)?)? as i32;
    let subkey = match field(&fields, 0xA2) {
        Ok(c) => Some(decode_enc_key(c)?),
        Err(_) => None,
    };
    let seq_number = match field(&fields, 0xA3) {
        Ok(c) => Some(decode_ctx_int(c)? as u32),
        Err(_) => None,
    };
    Ok(EncApRepPart {
        ctime,
        cusec,
        subkey,
        seq_number,
    })
}

/// 解码 AP-REQ, 返回 (Ticket, 加密的 Authenticator)。
pub fn decode_ap_req(data: &[u8]) -> Result<(Ticket, EncryptedData)> {
    let (tag, c, _) = take_tlv(data)?;
    if tag != 0x6E {
        return Err(KerberosError::Asn1(format!(
            "expected AP-REQ [APPLICATION 14] (0x6E), got 0x{tag:02x}"
        )));
    }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    let ticket = decode_ticket(field(&fields, 0xA3)?)?;
    let enc_authenticator = decode_enc_data(field(&fields, 0xA4)?)?;
    Ok((ticket, enc_authenticator))
}

/// 解码 AP-REP, 返回其 `enc-part` (用会话密钥 / subkey 解密后即为 `EncAPRepPart`)。
pub fn decode_ap_rep(data: &[u8]) -> Result<EncryptedData> {
    let (tag, c, _) = take_tlv(data)?;
    if tag != 0x6F {
        return Err(KerberosError::Asn1(format!(
            "expected AP-REP [APPLICATION 15] (0x6F), got 0x{tag:02x}"
        )));
    }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    decode_enc_data(field(&fields, 0xA2)?)
}

// ===========================================================================
// KDC 消息类型 (AS-REQ / TGS-REQ / AS-REP / TGS-REP)
// ===========================================================================

/// PA-DATA (RFC 4120 §5.2.6)
#[derive(Debug, Clone)]
pub struct PaData {
    pub padata_type: i32,
    pub padata_value: Vec<u8>, // 已编码的 OCTET STRING 内容 (不含 0x04 包装)
}

/// KDC-REQ-BODY (构建 AS-REQ / TGS-REQ 时使用)
pub struct KdcReqBody {
    pub kdc_options: [u8; 4], // 默认全零 (禁用全部 flag)
    pub cname: Option<PrincipalName>,
    pub realm: String,
    pub sname: Option<PrincipalName>,
    pub from: Option<String>,  // KerberosTime (可选)
    pub till: String,          // KerberosTime
    pub rtime: Option<String>, // renew-till (可选)
    pub nonce: i32,
    pub etype: Vec<i32>,
    pub addresses: Option<Vec<Vec<u8>>>, // 可选: addr-type + addr bytes
}

/// KDC-REP 解码后提取的信息 (AS-REP / TGS-REP 通用)
pub struct KdcRep {
    pub crealm: String,
    pub cname: PrincipalName,
    pub ticket: Ticket,
    pub enc_part: EncryptedData,
    /// 来自 KDC 的原始 Ticket DER (避免 decode/encode 回环改变字节)。
    pub ticket_der: Vec<u8>,
}

/// EncKdcRepPart: AS-REP 与 TGS-REP 的加密部分结构相同 (仅 APPLICATION tag 不同)
#[derive(Debug, Clone)]
pub struct EncKdcRepPart {
    pub key: EncryptionKey,
    pub nonce: i32,
    pub flags: i32,
    pub authtime: String,
    pub starttime: Option<String>,
    pub endtime: String,
    pub srealm: String,
    pub sname: PrincipalName,
}

// ----------------------------- KDC 编码 -----------------------------

/// 编码 PA-DATA。
pub fn encode_pa_data(pd: &PaData) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(1, pd.padata_type)); // padata-type [1]
    inner.extend(tlv_ctx(2, &tlv(0x04, &pd.padata_value))); // padata-value [2] OCTET STRING
    tlv_seq(&inner)
}

/// 编码 KDC-REQ-BODY (MIT krb5 实现约定)。
///
/// MIT krb5 使用的 context tag 编号与 RFC 4120 印刷版不同:
/// RFC `[6]nonce` → MIT `[7]nonce`, RFC `[7]etype` → MIT `[8]etype` 等。
pub fn encode_kdc_req_body(body: &KdcReqBody) -> Vec<u8> {
    let mut inner = Vec::new();
    // [0] kdc-options: BIT STRING (1 byte unused + 4 bytes flags)
    inner.extend(tlv_ctx(
        0,
        &tlv(
            0x03,
            &[
                0x00,
                body.kdc_options[0],
                body.kdc_options[1],
                body.kdc_options[2],
                body.kdc_options[3],
            ],
        ),
    ));
    // [1] cname (optional)
    if let Some(ref cname) = body.cname {
        inner.extend(tlv_ctx(1, &encode_principal_name(cname)));
    }
    // [2] realm (GeneralString)
    inner.extend(tlv_ctx(2, &tlv(0x1B, body.realm.as_bytes())));
    // [3] sname (optional)
    if let Some(ref sname) = body.sname {
        inner.extend(tlv_ctx(3, &encode_principal_name(sname)));
    }
    // [4] from (optional KerberosTime)
    if let Some(ref from) = body.from {
        inner.extend(tlv_ctx(4, &tlv(0x18, from.as_bytes())));
    }
    // [5] till (KerberosTime = GeneralizedTime)
    inner.extend(tlv_ctx(5, &tlv(0x18, body.till.as_bytes())));
    // [6] rtime (optional, renew-till)
    if let Some(ref rtime) = body.rtime {
        inner.extend(tlv_ctx(6, &tlv(0x18, rtime.as_bytes())));
    }
    // [7] nonce (INTEGER)
    inner.extend(ctx_int(7, body.nonce));
    // [8] etype (SEQUENCE OF INTEGER) — MIT 用 [8], 非 RFC [7]
    let mut etypes = Vec::new();
    for et in &body.etype {
        etypes.extend(tlv(0x02, &encode_int(*et)));
    }
    inner.extend(tlv_ctx(8, &tlv_seq(&etypes)));
    // [9] addresses (optional SEQUENCE OF HostAddress) — MIT 用 [9]
    if let Some(ref addrs) = body.addresses {
        let mut addr_seq = Vec::new();
        for addr_type_bytes in addrs {
            // HostAddress ::= SEQUENCE { addr-type [0], address [1] }
            let addr_type = addr_type_bytes[0];
            let addr = &addr_type_bytes[1..];
            let mut ha = Vec::new();
            ha.extend(ctx_int(0, addr_type as i32));
            ha.extend(tlv_ctx(1, &tlv(0x04, addr)));
            addr_seq.extend(tlv_seq(&ha));
        }
        inner.extend(tlv_ctx(9, &tlv_seq(&addr_seq)));
    }
    tlv_seq(&inner)
}

/// 编码 KDC-REQ (AS-REQ = [APPLICATION 10], TGS-REQ = [APPLICATION 12])。
///
/// `app_tag`: `TAG_AS_REQ` 或 `TAG_TGS_REQ`
/// `padata`: PA-DATA 列表 (已编码的 DER SEQUENCE OF PA-DATA), 传入 None 则省略。
///
/// MIT krb5 使用 `[1]pvno, [2]msg-type, [3]padata, [4]req-body` 的 context tag 编号,
/// 且 APPLICATION tag 包在 SEQUENCE 外层: `[APPL n] { SEQUENCE { fields } }`。
pub fn encode_kdc_req(
    pvno: i32,
    msg_type: i32,
    app_tag: u8,
    padata_der: Option<&[u8]>,
    req_body_der: &[u8],
) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(ctx_int(1, pvno));
    inner.extend(ctx_int(2, msg_type));
    if let Some(pd) = padata_der {
        inner.extend(tlv_ctx(3, pd));
    }
    inner.extend(tlv_ctx(4, req_body_der));
    tlv(app_tag, &tlv_seq(&inner))
}

/// 编码 EncKdcRepPart: AS-REP (TAG_ENC_AS_REP_PART) / TGS-REP (TAG_ENC_TGS_REP_PART)。
pub fn encode_enc_kdc_rep_part(part: &EncKdcRepPart, app_tag: u8) -> Vec<u8> {
    let mut inner = Vec::new();
    inner.extend(tlv_ctx(0, &encode_enc_key(&part.key)));
    inner.extend(ctx_int(2, part.nonce));
    inner.extend(ctx_int(4, part.flags));
    inner.extend(tlv_ctx(5, &tlv(0x18, part.authtime.as_bytes())));
    if let Some(ref st) = part.starttime {
        inner.extend(tlv_ctx(6, &tlv(0x18, st.as_bytes())));
    }
    inner.extend(tlv_ctx(7, &tlv(0x18, part.endtime.as_bytes())));
    inner.extend(tlv_ctx(9, &tlv(0x1B, part.srealm.as_bytes())));
    inner.extend(tlv_ctx(10, &encode_principal_name(&part.sname)));
    tlv(app_tag, &tlv_seq(&inner))
}

// ----------------------------- KDC 解码 -----------------------------

/// 解码 KDC-REP (AS-REP = [APPLICATION 11], TGS-REP = [APPLICATION 13])。
pub fn decode_kdc_rep(data: &[u8], expected_tag: u8) -> Result<KdcRep> {
    let (tag, c, _) = take_tlv(data)?;
    if tag != expected_tag {
        return Err(KerberosError::Asn1(format!(
            "expected KDC-REP 0x{expected_tag:02x}, got 0x{tag:02x}"
        )));
    }
    // [APPL n] { SEQUENCE { fields } }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    let _pvno = decode_int(field(&fields, 0xA0)?)?;
    let _msg_type = decode_int(field(&fields, 0xA1)?)?;
    // [3] crealm
    let crealm = {
        let rc = field(&fields, 0xA3)?;
        let (_t, s, _) = take_tlv(rc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    // [4] cname
    let cname = decode_principal_name(field(&fields, 0xA4)?)?;
    // [5] ticket — 保存原始 DER 避免 decode/encode 回环改变字节
    let ticket_raw = field(&fields, 0xA5)?;
    let ticket = decode_ticket(ticket_raw)?;
    let ticket_der = ticket_raw.to_vec();
    // [6] enc-part
    let enc_part = decode_enc_data(field(&fields, 0xA6)?)?;
    Ok(KdcRep {
        crealm,
        cname,
        ticket,
        enc_part,
        ticket_der,
    })
}

// ----------------------------- KRB-ERROR 解码 -----------------------------

/// KRB-ERROR 的 e-data 中提取的 PA-DATA 列表 (仅用于预认证协商)。
pub struct KdcErrorData {
    pub error_code: i32,
    pub etype_info: Vec<(i32, String)>, // (etype, salt)
}

/// 解码 KRB-ERROR (APPLICATION 30), 提取错误码与 PA-ETYPE-INFO2。
///
/// RFC 4120 §5.9.1 定义的 KRB-ERROR (MIT krb5 实现):
///   [0] pvno, [1] msg-type, [2] ctime(opt), [3] cusec(opt),
///   [4] stime, [5] susec, [6] error-code,
///   [7] crealm(opt), [8] cname(opt), [9] realm, [10] sname,
///   [11] e-text(opt), [12] e-data(opt)
pub fn decode_krb_error(data: &[u8]) -> Result<KdcErrorData> {
    let (tag, c, _) = take_tlv(data)?;
    if tag != 0x7E {
        return Err(KerberosError::Asn1(format!(
            "expected KRB-ERROR 0x7E, got 0x{tag:02x}"
        )));
    }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    // error-code = [6] (RFC 4120 §5.9.1)
    let error_code = decode_ctx_int(field(&fields, 0xA6)?)? as i32;

    // e-data = [12]; 也尝试 [13] 以防有 KDC 使用非标准 tag
    let edata_raw = field(&fields, 0xAC).or_else(|_| field(&fields, 0xAD));
    let etype_info = if let Ok(edata_raw) = edata_raw {
        parse_etype_info(edata_raw)?
    } else {
        Vec::new()
    };

    Ok(KdcErrorData {
        error_code,
        etype_info,
    })
}

/// 解析 KRB-ERROR 的 e-data 字段，提取 ETYPE-INFO 条目。
///
/// e-data 是 OCTET STRING，内部为 METHOD-DATA (SEQUENCE OF PA-DATA)。
/// 只关注 PA-ETYPE-INFO2 (19) 和 PA-ETYPE-INFO (23)。
fn parse_etype_info(edata_raw: &[u8]) -> Result<Vec<(i32, String)>> {
    let mut etype_info = Vec::new();

    // e-data 是 OCTET STRING, 内部为 METHOD-DATA (SEQUENCE OF PA-DATA)
    let (_etag, edata, _) = take_tlv(edata_raw)?;
    // edata 是 OCTET STRING 内容, 包含 SEQUENCE OF PA-DATA
    // 先取外层 SEQUENCE
    let pa_entries = collect_tlvs(edata)?;
    for (_, seq_content) in &pa_entries {
        // seq_content = content of the outer SEQUENCE, containing individual PA-DATA SEQUENCES
        // Each PA-DATA is a SEQUENCE (0x30) with inner [1] and [2] fields
        let mut rest = seq_content.as_slice();
        while !rest.is_empty() {
            let (pa_tag, pa_content, r) = take_tlv(rest)?;
            rest = r;
            if pa_tag != 0x30 {
                continue;
            }
            // pa_content = content of one PA-DATA SEQUENCE, parse [1] and [2]
            let pd_fields = collect_tlvs(pa_content)?;
            let pa_type = match field(&pd_fields, 0xA1) {
                Ok(c) => decode_ctx_int(c)? as i32,
                Err(_) => continue,
            };
            if pa_type == 19 || pa_type == 23 {
                // PA-ETYPE-INFO2 (19) or PA-ETYPE-INFO (23)
                if let Ok(pa_body_raw) = field(&pd_fields, 0xA2) {
                    let (_btag, pa_body, _) = take_tlv(pa_body_raw)?;
                    let entries = collect_tlvs(pa_body)?;
                    for (_, entry_val) in &entries {
                        let ei_fields = collect_tlvs(entry_val)?;
                        if let Ok(et_raw) = field(&ei_fields, 0xA0) {
                            let et = decode_ctx_int(et_raw)? as i32;
                            let salt = match field(&ei_fields, 0xA1) {
                                Ok(sr) => {
                                    let (_st, s, _) = take_tlv(sr)?;
                                    String::from_utf8_lossy(s).into_owned()
                                }
                                Err(_) => String::new(),
                            };
                            etype_info.push((et, salt));
                        }
                    }
                }
            }
        }
    }

    Ok(etype_info)
}

/// 解码 EncKdcRepPart (AS_REP / TGS_REP), 移除 APPLICATION 包装。
pub fn decode_enc_kdc_rep_part(data: &[u8]) -> Result<EncKdcRepPart> {
    let (tag, c, _) = take_tlv(data)?;
    if tag != 0x79 && tag != 0x7A {
        return Err(KerberosError::Asn1(format!(
            "expected EncKdcRepPart 0x79/0x7A, got 0x{tag:02x}"
        )));
    }
    // Unwrap outer SEQUENCE: [APPL n] { SEQUENCE { fields } }
    let (_, seq_content, _) = take_tlv(c)?;
    let fields = collect_tlvs(seq_content)?;
    let key = decode_enc_key(field(&fields, 0xA0)?)?;
    let nonce = decode_ctx_int(field(&fields, 0xA2)?)? as i32;
    let flags = decode_ctx_int(field(&fields, 0xA4)?)? as i32;
    let authtime = {
        let cc = field(&fields, 0xA5)?;
        let (_t, s, _) = take_tlv(cc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let starttime = match field(&fields, 0xA6) {
        Ok(cc) => {
            let (_t, s, _) = take_tlv(cc)?;
            Some(String::from_utf8_lossy(s).into_owned())
        }
        Err(_) => None,
    };
    let endtime = {
        let cc = field(&fields, 0xA7)?;
        let (_t, s, _) = take_tlv(cc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let srealm = {
        let rc = field(&fields, 0xA9)?;
        let (_t, s, _) = take_tlv(rc)?;
        String::from_utf8_lossy(s).into_owned()
    };
    let sname = decode_principal_name(field(&fields, 0xAA)?)?;
    Ok(EncKdcRepPart {
        key,
        nonce,
        flags,
        authtime,
        starttime,
        endtime,
        srealm,
        sname,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_principal() -> PrincipalName {
        PrincipalName {
            name_type: 1,
            name_string: vec!["kafka".to_string(), "broker.example.com".to_string()],
        }
    }

    #[test]
    fn principal_name_roundtrip() {
        let pn = sample_principal();
        let der = encode_principal_name(&pn);
        let decoded = decode_principal_name(&der).unwrap();
        assert_eq!(decoded.name_type, 1);
        assert_eq!(decoded.name_string, pn.name_string);
    }

    #[test]
    fn ticket_roundtrip() {
        let t = Ticket {
            tkt_vno: 5,
            realm: "EXAMPLE.COM".into(),
            sname: sample_principal(),
            enc_part: EncryptedData {
                etype: 18,
                kvno: Some(2),
                cipher: vec![0xDE, 0xAD, 0xBE, 0xEF],
            },
        };
        let der = encode_ticket(&t);
        let decoded = decode_ticket(&der).unwrap();
        assert_eq!(decoded.realm, "EXAMPLE.COM");
        assert_eq!(decoded.tkt_vno, 5);
        assert_eq!(decoded.sname.name_string, t.sname.name_string);
        assert_eq!(decoded.enc_part.etype, 18);
        assert_eq!(decoded.enc_part.kvno, Some(2));
        assert_eq!(decoded.enc_part.cipher, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn authenticator_roundtrip() {
        let a = Authenticator {
            authenticator_vno: 5,
            crealm: "EXAMPLE.COM".into(),
            cname: PrincipalName {
                name_type: 1,
                name_string: vec!["client".to_string()],
            },
            cksum: None,
            cusec: 123456,
            ctime: "20260713".to_string() + "101112Z",
            subkey: Some(EncryptionKey {
                keytype: 18,
                keyvalue: vec![0x01; 32],
            }),
            seq_number: Some(42),
        };
        let der = encode_authenticator(&a);
        let decoded = decode_authenticator(&der).unwrap();
        assert_eq!(decoded.crealm, "EXAMPLE.COM");
        assert_eq!(decoded.cname.name_string, vec!["client".to_string()]);
        assert_eq!(decoded.cusec, 123456);
        assert_eq!(decoded.ctime, a.ctime);
        assert_eq!(decoded.seq_number, Some(42));
        assert!(decoded.subkey.is_some());
    }

    #[test]
    fn ap_req_roundtrip() {
        let ticket = encode_ticket(&Ticket {
            tkt_vno: 5,
            realm: "EXAMPLE.COM".into(),
            sname: sample_principal(),
            enc_part: EncryptedData {
                etype: 18,
                kvno: None,
                cipher: vec![0x11; 16],
            },
        });
        let enc_auth = EncryptedData {
            etype: 18,
            kvno: None,
            cipher: vec![0x22; 24],
        };
        let der = encode_ap_req(&ticket, &enc_auth);
        let (dec_ticket, dec_auth) = decode_ap_req(&der).unwrap();
        assert_eq!(dec_ticket.realm, "EXAMPLE.COM");
        assert_eq!(dec_auth.cipher, vec![0x22; 24]);
    }
}
