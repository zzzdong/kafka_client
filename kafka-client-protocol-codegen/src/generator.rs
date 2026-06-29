use crate::types::{ParsedField, ParsedMessage};
use crate::utils::escape_string;
use crate::{InlineStructInfo, ParsedStruct};
use anyhow::Result;
use inflector::Inflector;
use std::fs;
use std::path::Path;

/// 目标类型
pub enum MessageType {
    Request,
    Response,
    Struct,
}

/// 生成 API 模块
pub fn generate_api_module(
    messages: &[ParsedMessage],
    structs: &[ParsedStruct],
    output_dir: &Path,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    // 生成共享结构体（只有 Message，无 api_key）
    for s in structs {
        generate_struct_file(s, output_dir)?;
    }

    // 生成消息文件（包含内联结构体）
    for msg in messages {
        generate_message_file(msg, output_dir)?;
    }

    // 生成 mod.rs
    generate_mod_file(messages, structs, output_dir)?;

    // 生成版本映射
    generate_version_map_file(messages, output_dir)?;

    Ok(())
}

/// 生成消息文件
fn generate_message_file(msg: &ParsedMessage, output_dir: &Path) -> Result<()> {
    let file_name = msg.name.to_snake_case();
    let file_path = output_dir.join(format!("{}.rs", file_name));

    let msg_type = determine_message_type(msg);

    let mut content = String::new();
    content.push_str("//! Auto-generated from Kafka protocol\n");
    content.push_str(&format!("//! Message: {}\n", msg.name));
    content.push_str("//! DO NOT EDIT\n\n");
    content.push_str("use kafka_client_protocol_core::{KafkaMessage, RecordBatch};\n");
    content.push_str("use bytes::Bytes;\n");
    content.push_str("use uuid::Uuid;\n\n");

    // 先生成所有内联结构体（只实现 Message）
    for s in &msg.inline_structs {
        generate_inline_struct_def(&mut content, s);
        content.push('\n');
    }

    // 生成主消息结构体
    generate_main_struct_def(&mut content, msg, msg_type);

    fs::write(file_path, content)?;
    Ok(())
}

/// 确定消息类型
fn determine_message_type(msg: &ParsedMessage) -> MessageType {
    match msg.msg_type.as_str() {
        "request" => MessageType::Request,
        "response" => MessageType::Response,
        "struct" => MessageType::Struct,
        _ => MessageType::Struct,
    }
}

/// 生成主消息结构体定义
fn generate_main_struct_def(content: &mut String, msg: &ParsedMessage, _msg_type: MessageType) {
    let _has_api_key = msg.api_key.is_some();

    // 结构体属性
    let mut attrs = Vec::new();
    if let Some(api_key) = msg.api_key {
        attrs.push(format!("api_key = {}", api_key));
    }
    // 添加 msg_type 属性
    attrs.push(format!("msg_type = \"{}\"", msg.msg_type));
    attrs.push(format!(
        "valid_versions = \"{}\"",
        escape_string(&msg.valid_versions)
    ));
    if let Some(flex) = &msg.flexible_versions {
        attrs.push(format!("flexible_versions = \"{}\"", escape_string(flex)));
    }

    if let Some(about) = &msg.about {
        content.push_str(&format!("/// {}\n", about));
    }

    content.push_str("#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]\n");
    if !attrs.is_empty() {
        content.push_str(&format!("#[kafka({})]\n", attrs.join(", ")));
    }
    content.push_str(&format!("pub struct {} {{\n", msg.struct_name));

    for field in &msg.fields {
        generate_field(content, field);
    }
    content.push_str("}\n\n");

    // 注意：trait 实现由派生宏自动生成，不需要在这里手动生成
    // 派生宏会根据 #[kafka(msg_type = "...")] 自动实现 Request 或 Response
}

/// 生成内联结构体定义（只实现 Message）
fn generate_inline_struct_def(content: &mut String, s: &InlineStructInfo) {
    content.push_str("#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]\n");
    content.push_str(&format!("pub struct {} {{\n", s.struct_name));
    for field in &s.fields {
        generate_field(content, field);
    }
    content.push_str("}\n\n");
}

/// 生成字段定义
fn generate_field(content: &mut String, field: &ParsedField) {
    if let Some(about) = &field.about {
        content.push_str(&format!("    /// {}\n", about));
    }

    let mut attrs = Vec::new();
    attrs.push(format!("versions = \"{}\"", escape_string(&field.versions)));

    if let Some(nv) = &field.nullable_versions {
        attrs.push(format!("nullable_versions = \"{}\"", escape_string(nv)));
    }
    if let Some(tag) = field.tag {
        attrs.push(format!("tag = {}", tag));
    }
    if let Some(tv) = &field.tagged_versions {
        attrs.push(format!("tagged_versions = \"{}\"", escape_string(tv)));
    }

    if let Some(def) = &field.default {
        let default_expr = format_default_value(def, &field.rust_type);
        attrs.push(format!("default = {}", default_expr));
    }

    if field.map_key {
        attrs.push("map_key".to_string());
    }

    // bytes→struct 映射：线格式中用 bytes 长度前缀，实际类型是结构体
    if field.rust_type.contains("RecordBatch") {
        attrs.push("encoded_as_bytes".to_string());
    }

    content.push_str(&format!("    #[kafka({})]\n", attrs.join(", ")));
    content.push_str(&format!(
        "    pub {}: {},\n",
        field.rust_name, field.rust_type
    ));
}

/// 根据 Rust 类型格式化默认值
fn format_default_value(default: &str, rust_type: &str) -> String {
    // 处理 null
    if default == "null" || default == "None" {
        return "None".to_string();
    }

    // 处理空字符串
    if default == "\"\"" && (rust_type == "String" || rust_type == "Option<String>") {
        return "String::new()".to_string();
    }

    // 根据目标类型格式化
    match rust_type {
        // 整数类型
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" => {
            // 十六进制格式如 "0x7fffffff"
            if default.starts_with("0x") {
                return default.to_string();
            }
            // 尝试解析为整数
            if let Ok(num) = default.parse::<i64>() {
                return num.to_string();
            }
            // 其他情况，直接使用原始字符串
            default.to_string()
        }
        // 浮点类型
        "f32" | "f64" => {
            if let Ok(num) = default.parse::<f64>() {
                return num.to_string();
            }
            default.to_string()
        }
        // 布尔类型
        "bool" => match default {
            "true" => "true".to_string(),
            "false" => "false".to_string(),
            _ => default.to_string(),
        },
        // 字符串类型
        "String" => {
            format!("\"{}\"", escape_string(default))
        }
        // Option<String>
        "Option<String>" => {
            if default == "null" {
                "None".to_string()
            } else if default == "\"\"" {
                "Some(String::new())".to_string()
            } else {
                format!("Some(\"{}\")", escape_string(default))
            }
        }
        // Option 类型（非 String）
        s if s.starts_with("Option<") => {
            if default == "null" {
                return "None".to_string();
            }
            let inner = s.trim_start_matches("Option<").trim_end_matches('>');
            let inner_default = format_default_value(default, inner);
            format!("Some({})", inner_default)
        }
        // Vec 类型
        s if s.starts_with("Vec<") => {
            if default == "null" || default == "None" {
                return "Vec::new()".to_string();
            }
            default.to_string()
        }
        // 其他类型（如结构体）
        _ => {
            if default == "null" || default == "None" {
                return "None".to_string();
            }
            default.to_string()
        }
    }
}

/// 生成共享结构体文件
fn generate_struct_file(s: &ParsedStruct, output_dir: &Path) -> Result<()> {
    let file_name = s.name.to_snake_case();
    let file_path = output_dir.join(format!("{}.rs", file_name));

    let mut content = String::new();
    content.push_str("//! Auto-generated common struct\n");
    content.push_str(&format!("//! Struct: {}\n", s.name));
    content.push_str("//! DO NOT EDIT\n\n");
    content.push_str("use kafka_client_protocol_core::Message;\n");
    content.push_str("use bytes::Bytes;\n");
    content.push_str("use uuid::Uuid;\n\n");

    if let Some(about) = &s.about {
        content.push_str(&format!("/// {}\n", about));
    }

    // 共享结构体只需要 valid_versions，不需要 api_key 和 msg_type
    let mut attrs = Vec::new();
    attrs.push(format!(
        "valid_versions = \"{}\"",
        escape_string(&s.valid_versions)
    ));
    if let Some(flex) = &s.flexible_versions {
        attrs.push(format!("flexible_versions = \"{}\"", escape_string(flex)));
    }

    content.push_str("#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]\n");
    if !attrs.is_empty() {
        content.push_str(&format!("#[kafka({})]\n", attrs.join(", ")));
    }
    content.push_str(&format!("pub struct {} {{\n", s.struct_name));

    for field in &s.fields {
        generate_field(&mut content, field);
    }

    content.push_str("}\n");

    fs::write(file_path, content)?;
    Ok(())
}

/// 生成 mod.rs
fn generate_mod_file(
    messages: &[ParsedMessage],
    structs: &[ParsedStruct],
    output_dir: &Path,
) -> Result<()> {
    let file_path = output_dir.join("mod.rs");
    let mut content = String::new();

    content.push_str("//! Auto-generated Kafka protocol messages\n");
    content.push_str("//! DO NOT EDIT\n\n");

    for s in structs {
        let file_name = s.name.to_snake_case();
        content.push_str(&format!("mod {};\n", file_name));
        content.push_str(&format!("pub use {}::{};\n", file_name, s.struct_name));
    }

    for msg in messages {
        let file_name = msg.name.to_snake_case();
        content.push_str(&format!("pub mod {};\n", file_name));
        content.push_str(&format!("pub use {}::{};\n", file_name, msg.struct_name));
    }

    content.push_str(&format!("mod {};\n", "version_map"));
    content.push_str(&format!("pub use {}::*;\n", "version_map"));

    fs::write(file_path, content)?;
    Ok(())
}

/// 生成版本映射文件
fn generate_version_map_file(messages: &[ParsedMessage], output_dir: &Path) -> Result<()> {
    let file_path = output_dir.join("version_map.rs");
    let mut content = String::new();

    content.push_str("//! API version mapping\n");
    content.push_str("//! DO NOT EDIT\n\n");

    // 生成静态数组（按 API Key 编号排序）
    content.push_str("/// 客户端支持的 API 版本范围（静态数组）\n");
    content.push_str("pub const CLIENT_SUPPORTED_VERSIONS: &[(i16, i16, i16)] = &[\n");

    let mut version_entries: Vec<(&ParsedMessage, i16, i16, i16)> = messages
        .iter()
        .filter_map(|msg| msg.api_key.map(|key| (msg, key)))
        .map(|(msg, key)| {
            let (min, max) = parse_version_range(&msg.valid_versions);
            (msg, key, min, max)
        })
        .collect();
    version_entries.sort_by_key(|e| e.1);

    for (msg, api_key, min, max) in &version_entries {
        content.push_str(&format!(
            "    ({}, {}, {}), // {}\n",
            api_key, min, max, msg.struct_name
        ));
    }

    content.push_str("];\n\n");

    // 生成辅助函数（已按 api_key 排序，使用二分查找）
    content.push_str("/// 获取指定 API 的支持版本范围\n");
    content.push_str("pub fn get_version_range(api_key: i16) -> Option<(i16, i16)> {\n");
    content.push_str("    CLIENT_SUPPORTED_VERSIONS\n");
    content.push_str("        .binary_search_by_key(&api_key, |&(key, _, _)| key)\n");
    content.push_str("        .ok()\n");
    content.push_str("        .map(|idx| {\n");
    content.push_str("            let (_, min, max) = CLIENT_SUPPORTED_VERSIONS[idx];\n");
    content.push_str("            (min, max)\n");
    content.push_str("        })\n");
    content.push_str("}\n\n");

    content.push_str("/// 检查是否支持指定版本\n");
    content.push_str("pub fn supports_version(api_key: i16, version: i16) -> bool {\n");
    content.push_str("    CLIENT_SUPPORTED_VERSIONS\n");
    content.push_str("        .binary_search_by_key(&api_key, |&(key, _, _)| key)\n");
    content.push_str("        .ok()\n");
    content.push_str("        .map(|idx| {\n");
    content.push_str("            let (_, min, max) = CLIENT_SUPPORTED_VERSIONS[idx];\n");
    content.push_str("            version >= min && version <= max\n");
    content.push_str("        })\n");
    content.push_str("        .unwrap_or(false)\n");
    content.push_str("}\n");

    fs::write(file_path, content)?;
    Ok(())
}

fn parse_version_range(s: &str) -> (i16, i16) {
    if s.contains('-') {
        let parts: Vec<&str> = s.split('-').collect();
        let min = parts[0].parse::<i16>().unwrap_or(0);
        let max = parts[1].parse::<i16>().unwrap_or(i16::MAX);
        (min, max)
    } else if s.ends_with('+') {
        let min = s.trim_end_matches('+').parse::<i16>().unwrap_or(0);
        (min, i16::MAX)
    } else {
        let v = s.parse::<i16>().unwrap_or(0);
        (v, v)
    }
}
