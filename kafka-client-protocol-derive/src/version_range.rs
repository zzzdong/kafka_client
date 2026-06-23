// src/version_range.rs
use proc_macro2::TokenStream;
use quote::quote;
use std::fmt;

/// 版本范围（支持 Kafka JSON 格式）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionRange {
    /// 精确版本，如 "0" 或 "3"
    Exact(i16),
    /// 闭区间，如 "0-12"
    Range(i16, i16),
    /// 从某版本开始，如 "3+"
    From(i16),
    /// 所有版本
    All,
}

impl VersionRange {
    /// 从字符串解析版本范围
    pub fn parse(s: &str) -> Self {
        let s = s.trim();

        // 空字符串或 "0+" 表示所有版本
        if s.is_empty() || s == "0+" || s == "none" || s == "all" {
            return VersionRange::All;
        }

        // 版本范围 "0-12"
        if let Some(idx) = s.find('-') {
            if let (Ok(start), Ok(end)) = (s[..idx].parse::<i16>(), s[idx + 1..].parse::<i16>()) {
                return VersionRange::Range(start, end);
            }
        }

        // 开放版本 "3+"
        if let Some(stripped) = s.strip_suffix('+') {
            if let Ok(start) = stripped.parse::<i16>() {
                return VersionRange::From(start);
            }
        }

        // 单个版本
        if let Ok(v) = s.parse::<i16>() {
            return VersionRange::Exact(v);
        }

        VersionRange::All
    }

    /// 检查版本是否在范围内
    #[allow(dead_code)]
    pub fn contains(&self, version: i16) -> bool {
        match self {
            VersionRange::Exact(v) => version == *v,
            VersionRange::Range(start, end) => version >= *start && version <= *end,
            VersionRange::From(start) => version >= *start,
            VersionRange::All => true,
        }
    }

    /// 获取最小版本
    pub fn min_version(&self) -> i16 {
        match self {
            VersionRange::Exact(v) => *v,
            VersionRange::Range(start, _) => *start,
            VersionRange::From(start) => *start,
            VersionRange::All => 0,
        }
    }

    /// 获取最大版本
    pub fn max_version(&self) -> Option<i16> {
        match self {
            VersionRange::Exact(v) => Some(*v),
            VersionRange::Range(_, end) => Some(*end),
            VersionRange::From(_) => None,
            VersionRange::All => None,
        }
    }

    /// 生成版本检查表达式
    pub fn as_check_expr(&self) -> TokenStream {
        match self {
            VersionRange::Exact(v) => quote! { version == #v },
            VersionRange::Range(start, end) => quote! { version >= #start && version <= #end },
            VersionRange::From(start) => quote! { version >= #start },
            VersionRange::All => quote! { true },
        }
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionRange::Exact(v) => write!(f, "{}", v),
            VersionRange::Range(s, e) => write!(f, "{}-{}", s, e),
            VersionRange::From(s) => write!(f, "{}+", s),
            VersionRange::All => write!(f, "0+"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version_range() {
        assert_eq!(VersionRange::parse("0"), VersionRange::Exact(0));
        assert_eq!(VersionRange::parse("5"), VersionRange::Exact(5));
        assert_eq!(VersionRange::parse("0-12"), VersionRange::Range(0, 12));
        assert_eq!(VersionRange::parse("4+"), VersionRange::From(4));
        assert_eq!(VersionRange::parse("0+"), VersionRange::All);
        assert_eq!(VersionRange::parse(""), VersionRange::All);
        assert_eq!(VersionRange::parse("none"), VersionRange::All);
    }

    #[test]
    fn test_version_contains() {
        let range = VersionRange::Range(4, 8);
        assert!(!range.contains(3));
        assert!(range.contains(4));
        assert!(range.contains(6));
        assert!(range.contains(8));
        assert!(!range.contains(9));

        let from = VersionRange::From(4);
        assert!(!from.contains(3));
        assert!(from.contains(4));
        assert!(from.contains(100));

        let exact = VersionRange::Exact(5);
        assert!(!exact.contains(4));
        assert!(exact.contains(5));
    }
}
