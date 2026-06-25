// src/utils.rs

/// Rust 关键字列表
pub const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for",
    "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
    "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use", "where",
    "while", "async", "await", "dyn", "abstract", "become", "box", "do", "final", "macro",
    "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "union",
];

/// 检查是否是 Rust 关键字
pub fn is_rust_keyword(s: &str) -> bool {
    RUST_KEYWORDS.contains(&s)
}

/// 安全地生成字段名（如果是关键字，添加 r# 前缀）
pub fn safe_field_name(name: &str) -> String {
    if is_rust_keyword(name) {
        format!("r#{}", name)
    } else {
        name.to_string()
    }
}

/// 将 Kafka 字段名转换为有效的 Rust 字段名
///
/// 规则：
/// - 已经是 snake_case 的保持不变（如 throttle_time_ms）
/// - camelCase 转换为 snake_case（如 throttleTimeMs -> throttle_time_ms）
/// - PascalCase 转换为 snake_case（如 ErrorCode -> error_code）
/// - 处理 Rust 关键字（如 type -> r#type）
pub fn to_field_name(s: &str) -> String {
    let snake = to_snake_case(s);
    safe_field_name(&snake)
}

/// 将字符串转换为 snake_case
///
/// 处理各种输入格式：
/// - camelCase: throttleTimeMs -> throttle_time_ms
/// - PascalCase: ErrorCode -> error_code
/// - snake_case: throttle_time_ms -> throttle_time_ms
/// - 大写缩写: KRaftVersionFeature -> k_raft_version_feature
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            // 检查是否是大写缩写的一部分
            let is_acronym = chars.peek().is_some_and(|&next| next.is_uppercase());

            if !result.is_empty() {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());

            if is_acronym {
                // 继续处理缩写中的其他大写字母
                while let Some(&next) = chars.peek() {
                    if next.is_uppercase() {
                        result.push(next.to_ascii_lowercase());
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }

    // 处理特殊情况：KRaft -> k_raft

    result.replace("k_raft", "k_raft")
}

/// 转换为 PascalCase（用于结构体名）
pub fn to_pascal_case(s: &str) -> String {
    // 先转换为 snake_case 再转换为 PascalCase
    let snake = to_snake_case(s);
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in snake.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    // 特殊处理：KRaft 保持为 KRaft
    if s.contains("KRaft") {
        result = result.replace("KRaft", "KRaft");
    }

    result
}

/// 映射 Kafka 类型到 Rust 类型
pub fn map_kafka_type_to_rust(
    field_type: &str,
    is_nullable: bool,
    default_is_null: bool,
) -> String {
    match field_type {
        "bool" => "bool".to_string(),
        "int8" => "i8".to_string(),
        "int16" => "i16".to_string(),
        "uint16" => "u16".to_string(),
        "int32" => "i32".to_string(),
        "uint32" => "u32".to_string(),
        "int64" => "i64".to_string(),
        "float64" => "f64".to_string(),
        "string" => {
            if is_nullable || default_is_null {
                "Option<String>".to_string()
            } else {
                "String".to_string()
            }
        }
        "uuid" => {
            if is_nullable || default_is_null {
                "Option<Uuid>".to_string()
            } else {
                "Uuid".to_string()
            }
        }
        "bytes" => {
            if is_nullable || default_is_null {
                "Option<Bytes>".to_string()
            } else {
                "Bytes".to_string()
            }
        }
        "records" => {
            if is_nullable || default_is_null {
                "Option<RecordBatch>".to_string()
            } else {
                "RecordBatch".to_string()
            }
        }
        s if s.starts_with("[]") => {
            let inner = &s[2..];
            let inner_type = map_kafka_type_to_rust(inner, false, false);
            if is_nullable || default_is_null {
                format!("Option<Vec<{}>>", inner_type)
            } else {
                format!("Vec<{}>", inner_type)
            }
        }
        s => {
            let pascal = to_pascal_case(s);
            if is_nullable || default_is_null {
                format!("Option<{}>", pascal)
            } else {
                pascal
            }
        }
    }
}

/// 转义字符串（用于属性值）
pub fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
