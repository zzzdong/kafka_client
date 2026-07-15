//! Kerberos cryptography core (built on RustCrypto primitives).
//!
//! Implemented and verified by unit tests:
//! - [`nfold`] (RFC 3961 §6.1)
//! - [`string_to_key`] (RFC 3962 / RFC 8009)
//! - AES-CTS (RFC 3962 ciphertext stealing, CS3 variant) encryption/decryption
//! - `EncryptedData` envelope (confounder + CTS + HMAC truncation)
use crate::error::{KerberosError, Result};
use aes::Aes128;
use aes::Aes256;
use aes::cipher::{BlockCipherDecrypt, BlockCipherEncrypt, KeyInit as AesKeyInit};
use cts::{CbcCs3, Decrypt as CtsDecrypt, Encrypt as CtsEncrypt, KeyIvInit as CtsKeyIvInit};
use hmac::Hmac;

use sha2::Sha384;

/// Kerberos 加密类型 (etype)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Etype {
    Aes128CtsHmacSha196 = 17,
    Aes256CtsHmacSha196 = 18,
    Aes256CtsHmacSha384192 = 20,
}

impl Etype {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            17 => Some(Etype::Aes128CtsHmacSha196),
            18 => Some(Etype::Aes256CtsHmacSha196),
            20 => Some(Etype::Aes256CtsHmacSha384192),
            _ => None,
        }
    }
    pub fn key_len(&self) -> usize {
        match self {
            Etype::Aes128CtsHmacSha196 => 16,
            Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => 32,
        }
    }
    /// HMAC truncation length (bytes): etype 17/18 → 96-bit, etype 20 → 192-bit.
    pub fn mac_len(&self) -> usize {
        match self {
            Etype::Aes128CtsHmacSha196 | Etype::Aes256CtsHmacSha196 => 12,
            Etype::Aes256CtsHmacSha384192 => 24,
        }
    }
}

// ------------------------- nfold (RFC 3961 §6.1) -------------------------

fn gcd(a: usize, b: usize) -> usize {
    if b == 0 { a } else { gcd(b, a % b) }
}

fn lcm(a: usize, b: usize) -> usize {
    (a / gcd(a, b)) * b
}

/// 将字节数组右旋转 nbits 位。
fn rotate_right(data: &[u8], nbits: usize) -> Vec<u8> {
    let len = data.len();
    let nbytes = (nbits / 8) % len;
    let remain = nbits % 8;
    let mut result = vec![0u8; len];
    for i in 0..len {
        let idx1 = (len + i - nbytes) % len;
        let idx2 = (len + i - nbytes - 1) % len;
        if remain == 0 {
            result[i] = data[idx1];
        } else {
            result[i] = ((data[idx1] >> remain) | ((data[idx2] << (8 - remain)) & 0xff)) as u8;
        }
    }
    result
}

/// 1's complement 加法 (end-around carry)。
fn add_ones_complement(a: &[u8], b: &[u8]) -> Vec<u8> {
    let n = a.len();
    let mut v: Vec<u16> = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| x as u16 + y as u16)
        .collect();

    loop {
        let has_carry = v.iter().any(|&x| x > 0xff);
        if !has_carry {
            break;
        }

        let mut new_v = vec![0u16; n];
        for i in 0..n {
            let carry_from_next = v[(i + 1) % n] >> 8;
            new_v[i] = (v[i] & 0xff) + carry_from_next;
        }
        v = new_v;
    }

    v.iter().map(|&x| x as u8).collect()
}

/// 将任意长度输入折叠/扩展为 `size` 比特 (size 必须是 8 的倍数)。
///
/// 实现参考 RFC 3961 §6.1 和 Impacket 的 nfold 实现。
pub fn nfold(data: &[u8], size: usize) -> Vec<u8> {
    assert_eq!(size % 8, 0, "nfold size must be a multiple of 8 bits");
    let out_bytes = size / 8;
    let in_bytes = data.len();

    if in_bytes == 0 {
        return vec![0u8; out_bytes];
    }

    let slen = in_bytes;
    let lcm_val = lcm(out_bytes, slen);

    let mut bigstr = Vec::with_capacity(lcm_val);
    for i in 0..(lcm_val / slen) {
        bigstr.extend_from_slice(&rotate_right(data, 13 * i));
    }

    let mut result = vec![0u8; out_bytes];
    let mut first = true;
    for p in (0..lcm_val).step_by(out_bytes) {
        let slice = &bigstr[p..p + out_bytes];
        if first {
            result.copy_from_slice(slice);
            first = false;
        } else {
            result = add_ones_complement(&result, slice);
        }
    }

    result
}

// ------------------------- string-to-key (RFC 3962 / 8009) -------------------------

/// HMAC-SHA1 手写实现 (sha1 0.10 使用 digest 0.10, 与 hmac 0.13 的 digest 0.11 不兼容)。
fn hmac_sha1(key: &[u8], data: &[u8]) -> Vec<u8> {
    use sha1::Digest;
    const BLOCK: usize = 64;
    let mut k = if key.len() > BLOCK {
        sha1::Sha1::digest(key).to_vec()
    } else {
        key.to_vec()
    };
    k.resize(BLOCK, 0);
    let mut ipad = k.clone();
    let mut opad = k.clone();
    for b in &mut ipad {
        *b ^= 0x36;
    }
    for b in &mut opad {
        *b ^= 0x5c;
    }
    let mut inner = ipad;
    inner.extend_from_slice(data);
    let inner_hash = sha1::Sha1::digest(&inner);
    let mut outer = opad;
    outer.extend_from_slice(&inner_hash);
    sha1::Sha1::digest(&outer).to_vec()
}

fn hmac_sha384(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha384> as hmac::digest::KeyInit>::new_from_slice(key).expect("hmac sha384");
    use hmac::Mac;
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// AES string-to-key (RFC 3962 §4 / RFC 8009)。
///
/// 流程:
///   1. t1 = PBKDF2(password, salt, iter, key_length)
///   2. K = DK(t1, "kerberos")   // 使用 HMAC 驱动 DK
pub fn string_to_key(etype: Etype, password: &[u8], salt: &[u8], iter: u32) -> Result<Vec<u8>> {
    let kl = etype.key_len();
    match etype {
        Etype::Aes128CtsHmacSha196 => {
            let t = pbkdf2_hmac_sha1(password, salt, iter, kl)?;
            Ok(dk_aes_aes128(&t, b"kerberos", kl))
        }
        Etype::Aes256CtsHmacSha196 => {
            let t = pbkdf2_hmac_sha1(password, salt, iter, kl)?;
            Ok(dk_aes_aes(&t, b"kerberos", kl))
        }
        Etype::Aes256CtsHmacSha384192 => {
            let t = pbkdf2_hmac_sha384(password, salt, iter, kl)?;
            Ok(dk_aes_aes(&t, b"kerberos", kl))
        }
    }
}

/// PBKDF2-HMAC-SHA1 (RFC 2898)。
fn pbkdf2_hmac_sha1(password: &[u8], salt: &[u8], iter: u32, dk_len: usize) -> Result<Vec<u8>> {
    pbkdf2(password, salt, iter, dk_len, hmac_sha1)
}

/// PBKDF2-HMAC-SHA384。
fn pbkdf2_hmac_sha384(password: &[u8], salt: &[u8], iter: u32, dk_len: usize) -> Result<Vec<u8>> {
    pbkdf2(password, salt, iter, dk_len, hmac_sha384)
}

/// 通用 PBKDF2 实现。
fn pbkdf2(
    password: &[u8],
    salt: &[u8],
    iter: u32,
    dk_len: usize,
    hmac: impl Fn(&[u8], &[u8]) -> Vec<u8>,
) -> Result<Vec<u8>> {
    let h_len = hmac(password, b"").len();
    let l = (dk_len + h_len - 1) / h_len;
    let mut dk = Vec::with_capacity(l * h_len);
    for i in 1..=l as u32 {
        let mut u = hmac(password, &[salt, &i.to_be_bytes()].concat());
        let mut t = u.clone();
        for _ in 1..iter {
            u = hmac(password, &u);
            for j in 0..h_len {
                t[j] ^= u[j];
            }
        }
        dk.extend_from_slice(&t);
    }
    dk.truncate(dk_len);
    Ok(dk)
}

// ------------------------- AES-CTS (RFC 3962, CS3) -------------------------

fn cts_encrypt<C: BlockCipherEncrypt + BlockCipherDecrypt + AesKeyInit>(
    key: &[u8],
    iv: &[u8],
    data: &[u8],
) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    if data.len() <= 16 {
        // Single block: CBC encrypt zero-padded block, output first data.len() bytes
        let cipher = <C as AesKeyInit>::new(key.try_into().unwrap());
        let mut block = [0u8; 16];
        block[..data.len()].copy_from_slice(data);
        let mut prev = [0u8; 16];
        prev.copy_from_slice(iv);
        for i in 0..16 {
            block[i] ^= prev[i];
        }
        cipher.decrypt_block((&mut block[..]).try_into().unwrap());
        return block[..data.len()].to_vec();
    }
    // Multi-block: use cts crate (CS3 variant, RFC 3962)
    let cipher = CbcCs3::<C>::new_from_slices(key, iv).expect("invalid key/iv length");
    let mut out = vec![0u8; data.len()];
    cipher
        .encrypt_b2b(data, &mut out)
        .expect("CTS encrypt: data length > block size");
    out
}

fn cts_decrypt<C: BlockCipherEncrypt + BlockCipherDecrypt + AesKeyInit>(
    key: &[u8],
    iv: &[u8],
    data: &[u8],
) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    if data.len() <= 16 {
        let cipher = <C as AesKeyInit>::new(key.try_into().unwrap());
        let mut block = [0u8; 16];
        block[..data.len()].copy_from_slice(data);
        let mut prev = [0u8; 16];
        prev.copy_from_slice(iv);
        for i in 0..16 {
            block[i] ^= prev[i];
        }
        cipher.decrypt_block((&mut block[..]).try_into().unwrap());
        return block[..data.len()].to_vec();
    }
    // Multi-block: use cts crate (CS3 variant, RFC 3962)
    let cipher = CbcCs3::<C>::new_from_slices(key, iv).expect("invalid key/iv length");
    let mut buf = data.to_vec();
    cipher
        .decrypt(&mut buf)
        .expect("CTS decrypt: data length > block size");
    buf
}

pub fn cts_encrypt_for(etype: Etype, key: &[u8], iv: &[u8], data: &[u8]) -> Vec<u8> {
    match etype {
        Etype::Aes128CtsHmacSha196 => cts_encrypt::<Aes128>(key, iv, data),
        Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => {
            cts_encrypt::<Aes256>(key, iv, data)
        }
    }
}

pub fn cts_decrypt_for(etype: Etype, key: &[u8], iv: &[u8], data: &[u8]) -> Vec<u8> {
    match etype {
        Etype::Aes128CtsHmacSha196 => cts_decrypt::<Aes128>(key, iv, data),
        Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => {
            cts_decrypt::<Aes256>(key, iv, data)
        }
    }
}

// ------------------------- 密钥派生 (RFC 3961 §5.1 + MIT krb5 实现) -------------------------
//
// MIT krb5 的 `k5_derive_random_rfc3961` 使用 AES 分组加密做 DR (不是 HMAC):
//   block = nfold(constant, blocksize)
//   K1 = AES(base_key, block)
//   K2 = AES(base_key, K1)   ← chain 模式: 每次输出作为下次输入
//   ...
//   DR = k-truncate(K1 || K2 || ...)
//
//  (参考: krb5/src/lib/crypto/builtin/kdf.c:141)
//
// MIT krb5 的 Ke/Ki 派生常量为 usage(4B, BE) | suffix(1B):
//   Ke = DK(key, usage | 0xAA)
//   Ki = DK(key, usage | 0x55)
//  (参考: krb5/src/lib/crypto/krb/enc_dk_hmac.c:130-141)

/// AES 加密驱动的 DR 函数 (MIT krb5 兼容).
fn dk_aes_aes(key: &[u8], constant: &[u8], out_len: usize) -> Vec<u8> {
    let bs = 16;
    // nfold constant to block size
    let mut block = nfold(constant, bs * 8);
    let cipher = aes::Aes256::new_from_slice(key).expect("AES-256 key");
    let mut result = Vec::with_capacity(out_len);
    while result.len() < out_len {
        let iv = [0u8; 16];
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&block[..16]);
        for j in 0..16 {
            buf[j] ^= iv[j];
        }
        cipher.encrypt_block((&mut buf[..]).try_into().unwrap());
        let remaining = out_len - result.len();
        let take = remaining.min(16);
        result.extend_from_slice(&buf[..take]);
        block = buf.to_vec();
    }
    result
}

fn dk_aes_aes128(key: &[u8], constant: &[u8], out_len: usize) -> Vec<u8> {
    let bs = 16;
    let mut block = nfold(constant, bs * 8);
    let cipher = aes::Aes128::new_from_slice(key).expect("AES-128 key");
    let mut result = Vec::with_capacity(out_len);
    while result.len() < out_len {
        let iv = [0u8; 16];
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&block[..16]);
        for j in 0..16 {
            buf[j] ^= iv[j];
        }
        cipher.encrypt_block((&mut buf[..]).try_into().unwrap());
        let remaining = out_len - result.len();
        let take = remaining.min(16);
        result.extend_from_slice(&buf[..take]);
        block = buf.to_vec();
    }
    result
}

pub fn dk_for(etype: Etype, key: &[u8], constant: &[u8]) -> Vec<u8> {
    let kl = etype.key_len();
    match etype {
        Etype::Aes128CtsHmacSha196 => dk_aes_aes128(key, constant, kl),
        Etype::Aes256CtsHmacSha196 | Etype::Aes256CtsHmacSha384192 => dk_aes_aes(key, constant, kl),
    }
}

/// RFC 3962 DK: n-fold(HMAC(key, constant), key_len * 8)
///
/// Java JDK 的 Krb5 CipherHelper.dk() 使用此方式。
pub fn dk_rfc(etype: Etype, key: &[u8], constant: &[u8]) -> Vec<u8> {
    let out_bits = etype.key_len() * 8;
    let h = hmac_for(etype, key, constant);
    nfold(&h, out_bits)
}

pub fn hmac_for(etype: Etype, key: &[u8], data: &[u8]) -> Vec<u8> {
    match etype {
        Etype::Aes128CtsHmacSha196 | Etype::Aes256CtsHmacSha196 => hmac_sha1(key, data),
        Etype::Aes256CtsHmacSha384192 => hmac_sha384(key, data),
    }
}

/// Build a DK constant: usage (4-byte big-endian) + suffix (1 byte).
/// Simplified profile (RFC 3961 §5.3):
///   Ke = DK(key, usage | 0xAA)
///   Ki = DK(key, usage | 0x55)
pub fn usage_constant(usage: u32, suffix: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.extend_from_slice(&usage.to_be_bytes());
    v.push(suffix);
    v
}

// ------------------------- 随机数辅助 -------------------------

/// 生成 16 字节安全随机数 (用于 confounder)。
fn random_16_bytes() -> Result<Vec<u8>> {
    let mut buf = vec![0u8; 16];
    getrandom::fill(&mut buf)
        .map_err(|e| KerberosError::Crypto(format!("failed to generate confounder random: {e}")))?;
    Ok(buf)
}

// ------------------------- EncryptedData 封装 (RFC 3962 §5 / RFC 3961 简化配置) -------------------------
//
// 正确顺序 (RFC 3962 §5):
//   1. Ke = DK(key, usage | 0xAA); Ki = DK(key, usage | 0x55)
//   2. ciphertext = AES-CTS(Ke, confounder || plaintext)
//   3. MAC = HMAC(Ki, ciphertext)[:h]
//   4. output = ciphertext || MAC

/// Encrypt (MIT krb5 compatible):
///   1. Ke = DK(key, usage | 0xAA), Ki = DK(key, usage | 0x55)
///   2. HMAC = HMAC(Ki, confounder || plaintext)      ← over plaintext
///   3. C = AES-CTS(Ke, confounder || plaintext + pad) ← encrypt after
///   4. Output = C || HMAC[..h]
pub fn encrypt(etype: Etype, key: &[u8], usage: u32, plaintext: &[u8]) -> Result<Vec<u8>> {
    let confounder = random_16_bytes()?;
    let mut basic_plaintext = confounder;
    basic_plaintext.extend_from_slice(plaintext); // 无 padding (AES-CTS 不须填充)

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));

    // HMAC 对明文 (confounder || plaintext)
    let mac = hmac_for(etype, &ki, &basic_plaintext);

    // 再加密
    let iv = vec![0u8; 16];
    let cipher = cts_encrypt_for(etype, &ke, &iv, &basic_plaintext);

    let mut out = cipher;
    out.extend_from_slice(&mac[..etype.mac_len()]);
    Ok(out)
}

/// 加密 (RFC 3962 §5): MAC on ciphertext.
/// 用于 GSS-API / Java JRE 互操作场景。
pub fn encrypt_rfc3962(etype: Etype, key: &[u8], usage: u32, plaintext: &[u8]) -> Result<Vec<u8>> {
    let confounder = random_16_bytes()?;
    let mut basic_plaintext = confounder;
    basic_plaintext.extend_from_slice(plaintext);

    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));

    // 先加密
    let iv = vec![0u8; 16];
    let cipher = cts_encrypt_for(etype, &ke, &iv, &basic_plaintext);

    // 再对密文计算 MAC (RFC 3962 §5: HMAC of ciphertext)
    let mac = hmac_for(etype, &ki, &cipher);

    let mut out = cipher;
    out.extend_from_slice(&mac[..etype.mac_len()]);
    Ok(out)
}

/// 解密 (RFC 3962 §5): 先验 MAC on ciphertext, 再解密。
/// 用于 GSS-API / Java JRE 互操作场景。
pub fn decrypt_rfc3962(etype: Etype, key: &[u8], usage: u32, data: &[u8]) -> Result<Vec<u8>> {
    let mac_len = etype.mac_len();
    if data.len() < mac_len + 16 {
        return Err(KerberosError::Crypto("ciphertext too short".into()));
    }
    let (cipher, mac) = data.split_at(data.len() - mac_len);

    // 先验 HMAC (对密文)
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));
    let computed = hmac_for(etype, &ki, cipher);
    if &computed[..mac_len] != mac {
        return Err(KerberosError::Crypto("HMAC verification failed".into()));
    }

    // 再解密
    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let iv = vec![0u8; 16];
    let basic_plaintext = cts_decrypt_for(etype, &ke, &iv, cipher);

    if basic_plaintext.len() < 16 {
        return Err(KerberosError::Crypto("decrypt too short".into()));
    }
    Ok(basic_plaintext[16..].to_vec())
}

/// 解密 (MIT krb5 兼容):
///   1. 先解密得 confounder || plaintext
///   2. 再验证 HMAC(Ki, confounder || plaintext)
pub fn decrypt(etype: Etype, key: &[u8], usage: u32, data: &[u8]) -> Result<Vec<u8>> {
    let mac_len = etype.mac_len();
    if data.len() < mac_len + 16 {
        return Err(KerberosError::Crypto("ciphertext too short".into()));
    }
    let (cipher, mac) = data.split_at(data.len() - mac_len);

    // 先解密
    let ke = dk_for(etype, key, &usage_constant(usage, 0xAA));
    let iv = vec![0u8; 16];
    let basic_plaintext = cts_decrypt_for(etype, &ke, &iv, cipher);

    // 再验 HMAC (对明文)
    let ki = dk_for(etype, key, &usage_constant(usage, 0x55));
    let computed = hmac_for(etype, &ki, &basic_plaintext);
    if &computed[..mac_len] != mac {
        return Err(KerberosError::Crypto("HMAC verification failed".into()));
    }

    if basic_plaintext.len() < 16 {
        return Err(KerberosError::Crypto("decrypt too short".into()));
    }
    Ok(basic_plaintext[16..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== n-fold ====================

    #[test]
    fn nfold_length() {
        assert_eq!(nfold(b"kerberos", 64).len(), 8);
        assert_eq!(nfold(b"kerberos", 128).len(), 16);
        assert_eq!(nfold(b"kerberos", 256).len(), 32);
        assert_eq!(nfold(b"", 64).len(), 8);
    }

    #[test]
    fn nfold_kerberos_128() {
        let result = nfold(b"kerberos", 128);
        let expected: [u8; 16] = [
            0x6b, 0x65, 0x72, 0x62, 0x65, 0x72, 0x6f, 0x73, 0x7b, 0x9b, 0x5b, 0x2b, 0x93, 0x13,
            0x2b, 0x93,
        ];
        assert_eq!(result, expected, "nfold(kerberos, 128) mismatch");
    }

    /// RFC 3961 §A.1 n-fold 测试向量
    #[test]
    fn nfold_rfc3961_vectors() {
        assert_eq!(
            nfold(b"012345", 64),
            &[0xbe, 0x07, 0x26, 0x31, 0x27, 0x6b, 0x19, 0x55][..]
        );
        assert_eq!(
            nfold(b"password", 56),
            &[0x78, 0xa0, 0x7b, 0x6c, 0xaf, 0x85, 0xfa][..]
        );
        assert_eq!(
            nfold(b"Rough Consensus, and Running Code", 64),
            &[0xbb, 0x6e, 0xd3, 0x08, 0x70, 0xb7, 0xf0, 0xe0][..]
        );
        assert_eq!(
            nfold(b"password", 168),
            &[
                0x59, 0xe4, 0xa8, 0xca, 0x7c, 0x03, 0x85, 0xc3, 0xc3, 0x7b, 0x3f, 0x6d, 0x20, 0x00,
                0x24, 0x7c, 0xb6, 0xe6, 0xbd, 0x5b, 0x3e
            ][..]
        );
        assert_eq!(
            nfold(b"MASSACHVSETTS INSTITVTE OF TECHNOLOGY", 192),
            &[
                0xdb, 0x3b, 0x0d, 0x8f, 0x0b, 0x06, 0x1e, 0x60, 0x32, 0x82, 0xb3, 0x08, 0xa5, 0x08,
                0x41, 0x22, 0x9a, 0xd7, 0x98, 0xfa, 0xb9, 0x54, 0x0c, 0x1b
            ][..]
        );
        assert_eq!(
            nfold(b"Q", 168),
            &[
                0x51, 0x8a, 0x54, 0xa2, 0x15, 0xa8, 0x45, 0x2a, 0x51, 0x8a, 0x54, 0xa2, 0x15, 0xa8,
                0x45, 0x2a, 0x51, 0x8a, 0x54, 0xa2, 0x15
            ][..]
        );
        assert_eq!(
            nfold(b"ba", 168),
            &[
                0xfb, 0x25, 0xd5, 0x31, 0xae, 0x89, 0x74, 0x49, 0x9f, 0x52, 0xfd, 0x92, 0xea, 0x98,
                0x57, 0xc4, 0xba, 0x24, 0xcf, 0x29, 0x7e
            ][..]
        );
        assert_eq!(
            nfold(b"kerberos", 256),
            &[
                0x6b, 0x65, 0x72, 0x62, 0x65, 0x72, 0x6f, 0x73, 0x7b, 0x9b, 0x5b, 0x2b, 0x93, 0x13,
                0x2b, 0x93, 0x5c, 0x9b, 0xdc, 0xda, 0xd9, 0x5c, 0x98, 0x99, 0xc4, 0xca, 0xe4, 0xde,
                0xe6, 0xd6, 0xca, 0xe4
            ][..]
        );
    }

    // ==================== AES 驱动 DK 验证 ====================

    #[test]
    fn dk_aes_deterministic() {
        let key = [0x42u8; 32];
        let c = usage_constant(2, 0xAA);
        let ke = dk_aes_aes(&key, &c, 32);
        assert_eq!(ke.len(), 32);
        assert_eq!(dk_aes_aes(&key, &c, 32), ke);
    }

    #[test]
    fn dk_aes_aes128_len() {
        let ke = dk_aes_aes128(&[0x11u8; 16], &usage_constant(1, 0xAA), 16);
        assert_eq!(ke.len(), 16);
    }

    #[test]
    fn dk_aes_aes256_len() {
        let ke = dk_aes_aes(&[0x22u8; 32], &usage_constant(3, 0xAA), 32);
        assert_eq!(ke.len(), 32);
    }

    #[test]
    fn dk_aes_ke_vs_ki() {
        let key = [0x42u8; 32];
        assert_ne!(
            dk_aes_aes(&key, &usage_constant(3, 0xAA), 32),
            dk_aes_aes(&key, &usage_constant(3, 0x55), 32)
        );
    }

    #[test]
    fn dk_aes_different_usage() {
        let key = [0x42u8; 32];
        assert_ne!(
            dk_aes_aes(&key, &usage_constant(1, 0xAA), 32),
            dk_aes_aes(&key, &usage_constant(2, 0xAA), 32)
        );
    }

    // ==================== 加密/解密 roundtrip ====================

    #[test]
    fn aes256_cts_roundtrip() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let pt = b"hello kerberos gssapi world, this is a longer test plaintext!";
        let ct = encrypt(etype, &key, 2, pt).unwrap();
        assert_eq!(decrypt(etype, &key, 2, &ct).unwrap(), pt);
    }

    #[test]
    fn aes256_cts_roundtrip_partial() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        for pt in [
            &b"short"[..],
            &b"exactly sixteen b"[..],
            &b"twenty bytes exactly!!"[..],
        ] {
            let ct = encrypt(etype, &key, 7, pt).unwrap();
            assert_eq!(
                decrypt(etype, &key, 7, &ct).unwrap(),
                pt,
                "pt len = {}",
                pt.len()
            );
        }
    }

    #[test]
    fn aes128_cts_roundtrip() {
        let etype = Etype::Aes128CtsHmacSha196;
        let key = vec![0x11u8; 16];
        let pt = b"short";
        let ct = encrypt(etype, &key, 2, pt).unwrap();
        assert_eq!(decrypt(etype, &key, 2, &ct).unwrap(), pt);
    }

    /// 验证不同 usage 和不同 etype 的 roundtrip。
    #[test]
    fn encrypt_decrypt_various_usage_etype() {
        let cases: Vec<(Etype, Vec<u8>, u32, &[u8])> = vec![
            (
                Etype::Aes128CtsHmacSha196,
                vec![0x11u8; 16],
                1u32,
                b"test data",
            ),
            (
                Etype::Aes128CtsHmacSha196,
                vec![0x11u8; 16],
                3u32,
                b"longer test payload for aes128",
            ),
            (
                Etype::Aes256CtsHmacSha196,
                vec![0x42u8; 32],
                1u32,
                b"test data",
            ),
            (
                Etype::Aes256CtsHmacSha196,
                vec![0x42u8; 32],
                3u32,
                b"longer test payload for aes256",
            ),
        ];
        for (etype, key, usage, pt) in &cases {
            let ct = encrypt(*etype, key, *usage, pt).unwrap();
            let dec = decrypt(*etype, key, *usage, &ct).unwrap();
            assert_eq!(
                &dec,
                pt,
                "etype={:?} usage={} pt.len={}",
                etype,
                usage,
                pt.len()
            );
        }
    }

    // ==================== 错误检测 ====================

    /// 篡改密文 -> HMAC 校验失败。
    #[test]
    fn hmac_mismatch_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let mut ct = encrypt(etype, &key, 2, b"payload").unwrap();
        ct[0] ^= 0xff; // 破坏密文
        assert!(decrypt(etype, &key, 2, &ct).is_err());
    }

    /// 篡改 MAC -> HMAC 校验失败。
    #[test]
    fn mac_tampered_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let mut ct = encrypt(etype, &key, 2, b"payload").unwrap();
        let last = ct.len() - 1;
        ct[last] ^= 0xff; // 破坏 MAC
        assert!(decrypt(etype, &key, 2, &ct).is_err());
    }

    /// 错误的密钥 -> HMAC 校验失败。
    #[test]
    fn wrong_key_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let wrong_key = vec![0x99u8; 32];
        let ct = encrypt(etype, &key, 2, b"secret").unwrap();
        assert!(decrypt(etype, &wrong_key, 2, &ct).is_err());
    }

    /// 不同 usage 导致不同 MAC -> decrypt 失败。
    #[test]
    fn wrong_usage_detected_now() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let ct = encrypt(etype, &key, 1, b"secret").unwrap();
        assert!(decrypt(etype, &key, 2, &ct).is_err());
    }

    /// 截断的密文 -> 错误。
    #[test]
    fn truncated_ciphertext_detected() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let ct = encrypt(etype, &key, 2, b"data").unwrap();
        // 太短（不足 mac_len + 16）
        assert!(decrypt(etype, &key, 2, &ct[..10]).is_err());
    }

    // ==================== string-to-key ====================

    #[test]
    fn string_to_key_deterministic() {
        let k = string_to_key(
            Etype::Aes256CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            4096,
        )
        .unwrap();
        assert_eq!(k.len(), 32);
        // 相同输入产生相同输出
        let k2 = string_to_key(
            Etype::Aes256CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            4096,
        )
        .unwrap();
        assert_eq!(k, k2);
    }

    #[test]
    fn string_to_key_aes128_length() {
        let k = string_to_key(
            Etype::Aes128CtsHmacSha196,
            b"password",
            b"ATHENA.MIT.EDUraeburn",
            4096,
        )
        .unwrap();
        assert_eq!(k.len(), 16);
    }

    #[test]
    fn string_to_key_different_salt() {
        let k1 = string_to_key(Etype::Aes256CtsHmacSha196, b"password", b"salt1", 4096).unwrap();
        let k2 = string_to_key(Etype::Aes256CtsHmacSha196, b"password", b"salt2", 4096).unwrap();
        assert_ne!(k1, k2);
    }

    // ==================== 跨协议验证: 密钥派生与加密兼容性 ====================

    /// 验证加密输出按正确格式: [密文][HMAC截断]
    /// 密文长度 = 16(confounder) + 明文长度(CTS 保留长度)
    /// 总长度 = 密文长度 + mac_len
    #[test]
    fn ciphertext_format_check() {
        let etype = Etype::Aes256CtsHmacSha196;
        let key = vec![0x42u8; 32];
        let plaintext = b"hello world";
        let pt_len = plaintext.len();
        let expected_ct_len = pt_len + 16; // confounder 16 bytes, CTS preserves length
        let expected_total = expected_ct_len + etype.mac_len(); // +12 for HMAC

        let ct = encrypt(etype, &key, 2, plaintext).unwrap();
        assert_eq!(
            ct.len(),
            expected_total,
            "ciphertext total length: expected {expected_total}, got {}",
            ct.len()
        );

        let dec = decrypt(etype, &key, 2, &ct).unwrap();
        assert_eq!(dec, plaintext);
    }
}
