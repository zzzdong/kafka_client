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
        content.push_str("\n");
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
fn generate_main_struct_def(content: &mut String, msg: &ParsedMessage, msg_type: MessageType) {
    let has_api_key = msg.api_key.is_some();

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

/// 生成 trait 实现
fn generate_trait_impls(
    content: &mut String,
    msg: &ParsedMessage,
    msg_type: MessageType,
    has_api_key: bool,
) {
    // 所有类型都实现 Message trait
    generate_message_impl(content, msg);

    // 如果消息有 api_key，根据类型实现 Request 或 Response
    if has_api_key {
        match msg_type {
            MessageType::Request => generate_request_impl(content, msg),
            MessageType::Response => generate_response_impl(content, msg),
            MessageType::Struct => {
                // 不应该发生，但如果有，只实现 Message
            }
        }
    }
}

/// 生成 Message trait 实现
fn generate_message_impl(content: &mut String, msg: &ParsedMessage) {
    let struct_name = &msg.struct_name;
    let type_name = &msg.name;
    let min_version = parse_min_version(&msg.valid_versions);
    let max_version = parse_max_version(&msg.valid_versions);
    let flexible_version = msg.flexible_versions.as_ref().map(|v| parse_min_version(v));

    content.push_str(&format!("impl Message for {} {{\n", struct_name));
    content.push_str(&format!("    fn type_name() -> &'static str {{\n"));
    content.push_str(&format!("        \"{}\"\n", type_name));
    content.push_str("    }\n\n");

    content.push_str("    fn max_version() -> i16 {\n");
    content.push_str(&format!("        {}\n", max_version));
    content.push_str("    }\n\n");

    content.push_str("    fn min_version() -> i16 {\n");
    content.push_str(&format!("        {}\n", min_version));
    content.push_str("    }\n\n");

    if let Some(flex) = flexible_version {
        content.push_str("    fn flexible_version() -> Option<i16> {\n");
        content.push_str(&format!("        Some({})\n", flex));
        content.push_str("    }\n\n");
    }

    // encode 方法
    content.push_str("    fn encode(&self, buf: &mut bytes::BytesMut, version: i16) -> crate::ProtocolResult<()> {\n");
    content.push_str("        use kafka_client_protocol_core::*;\n");
    content.push_str("        use bytes::BufMut;\n");
    for field in &msg.fields {
        generate_encode_field(content, field);
    }
    content.push_str("        Ok(())\n");
    content.push_str("    }\n\n");

    // decode 方法
    content.push_str(
        "    fn decode(buf: &mut bytes::Bytes, version: i16) -> crate::ProtocolResult<Self> {\n",
    );
    content.push_str("        use kafka_client_protocol_core::*;\n");
    content.push_str("        use bytes::Buf;\n");
    content.push_str("        let mut msg = Self::default();\n");
    for field in &msg.fields {
        generate_decode_field(content, field);
    }
    content.push_str("        Ok(msg)\n");
    content.push_str("    }\n\n");

    // size 方法
    content.push_str("    fn size(&self, version: i16) -> usize {\n");
    content.push_str("        use kafka_client_protocol_core::*;\n");
    content.push_str("        let mut total = 0;\n");
    for field in &msg.fields {
        generate_size_field(content, field);
    }
    content.push_str("        total\n");
    content.push_str("    }\n");

    content.push_str("}\n\n");
}

/// 生成 Request trait 实现
fn generate_request_impl(content: &mut String, msg: &ParsedMessage) {
    let struct_name = &msg.struct_name;
    let api_key = msg.api_key.unwrap();

    content.push_str(&format!("impl Request for {} {{\n", struct_name));
    content.push_str("    fn api_key(&self) -> i16 {\n");
    content.push_str(&format!("        {}\n", api_key));
    content.push_str("    }\n");
    content.push_str("}\n\n");
}

/// 生成 Response trait 实现
fn generate_response_impl(content: &mut String, msg: &ParsedMessage) {
    let struct_name = &msg.struct_name;
    let api_key = msg.api_key.unwrap();

    content.push_str(&format!("impl Response for {} {{\n", struct_name));
    content.push_str("    fn api_key(&self) -> i16 {\n");
    content.push_str(&format!("        {}\n", api_key));
    content.push_str("    }\n");
    content.push_str("}\n\n");
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

    content.push_str(&format!("    #[kafka({})]\n", attrs.join(", ")));
    content.push_str(&format!(
        "    pub {}: {},\n",
        field.rust_name, field.rust_type
    ));
}

/// 生成 encode 字段代码
fn generate_encode_field(content: &mut String, field: &ParsedField) {
    let condition = generate_version_condition(&field.versions);
    let rust_type = &field.rust_type;
    let field_name = &field.rust_name;
    let use_flexible = is_flexible_field(field);

    if field.versions != "0+" && field.versions != "all" {
        content.push_str(&format!("        if {} {{\n", condition));
        content.push_str(&generate_encode_body(
            field,
            rust_type,
            field_name,
            use_flexible,
        ));
        content.push_str("        }\n");
    } else {
        content.push_str(&generate_encode_body(
            field,
            rust_type,
            field_name,
            use_flexible,
        ));
    }
}

/// 生成 encode 体
fn generate_encode_body(
    field: &ParsedField,
    rust_type: &str,
    field_name: &str,
    use_flexible: bool,
) -> String {
    let mut code = String::new();

    // Option 类型特殊处理
    if rust_type.starts_with("Option<") {
        if let Some(nv) = &field.nullable_versions {
            let null_condition = generate_version_condition(nv);
            code.push_str(&format!(
                "            if {} && self.{}.is_none() {{\n",
                null_condition, field_name
            ));
            if use_flexible {
                code.push_str("                encode_unsigned_varint(buf, 0);\n");
            } else {
                code.push_str("                buf.put_i32(-1);\n");
            }
            code.push_str("            } else {\n");
            code.push_str(&format!(
                "                self.{}.encode(buf, version)?;\n",
                field_name
            ));
            code.push_str("            }\n");
            return code;
        }
        return format!("            self.{}.encode(buf, version)?;\n", field_name);
    }

    // 根据类型生成编码
    if rust_type == "String" {
        if use_flexible {
            code.push_str(&format!(
                "            encode_compact_string(buf, &self.{});\n",
                field_name
            ));
        } else {
            code.push_str(&format!(
                "            encode_string(buf, &self.{});\n",
                field_name
            ));
        }
    } else if rust_type == "Bytes" {
        if use_flexible {
            code.push_str(&format!(
                "            encode_compact_bytes(buf, &self.{});\n",
                field_name
            ));
        } else {
            code.push_str(&format!(
                "            encode_bytes(buf, &self.{});\n",
                field_name
            ));
        }
    } else if rust_type.starts_with("Vec<") {
        if use_flexible {
            code.push_str(&format!(
                "            encode_compact_array(buf, &self.{}, |b, item| item.encode(b, version))?;\n",
                field_name
            ));
        } else {
            code.push_str(&format!(
                "            encode_array(buf, &self.{}, |b, item| item.encode(b, version))?;\n",
                field_name
            ));
        }
    } else if rust_type == "bool" {
        code.push_str(&format!(
            "            buf.put_i8(self.{} as i8);\n",
            field_name
        ));
    } else if rust_type == "i8" {
        code.push_str(&format!("            buf.put_i8(self.{});\n", field_name));
    } else if rust_type == "i16" {
        code.push_str(&format!("            buf.put_i16(self.{});\n", field_name));
    } else if rust_type == "i32" {
        code.push_str(&format!("            buf.put_i32(self.{});\n", field_name));
    } else if rust_type == "i64" {
        code.push_str(&format!("            buf.put_i64(self.{});\n", field_name));
    } else if rust_type == "u8" {
        code.push_str(&format!("            buf.put_u8(self.{});\n", field_name));
    } else if rust_type == "u16" {
        code.push_str(&format!("            buf.put_u16(self.{});\n", field_name));
    } else if rust_type == "u32" {
        code.push_str(&format!("            buf.put_u32(self.{});\n", field_name));
    } else if rust_type == "u64" {
        code.push_str(&format!("            buf.put_u64(self.{});\n", field_name));
    } else if rust_type == "f64" {
        code.push_str(&format!("            buf.put_f64(self.{});\n", field_name));
    } else {
        // 自定义类型，使用 Message trait
        code.push_str(&format!(
            "            self.{}.encode(buf, version)?;\n",
            field_name
        ));
    }
    code
}

/// 生成 decode 字段代码
fn generate_decode_field(content: &mut String, field: &ParsedField) {
    let condition = generate_version_condition(&field.versions);
    let rust_type = &field.rust_type;
    let field_name = &field.rust_name;

    if field.versions != "0+" && field.versions != "all" {
        content.push_str(&format!("        if {} {{\n", condition));
        content.push_str(&format!(
            "            msg.{} = {};\n",
            field_name,
            generate_decode_body(field, rust_type)
        ));
        content.push_str("        }\n");
    } else {
        content.push_str(&format!(
            "        msg.{} = {};\n",
            field_name,
            generate_decode_body(field, rust_type)
        ));
    }
}

/// 生成 decode 体
fn generate_decode_body(field: &ParsedField, rust_type: &str) -> String {
    let use_flexible = is_flexible_field(field);

    // Option 类型特殊处理
    if rust_type.starts_with("Option<") {
        if let Some(nv) = &field.nullable_versions {
            let null_condition = generate_version_condition(nv);
            return format!(
                "if {} {{\n                if {} {{\n                    None\n                }} else {{\n                    Some({})\n                }}\n            }} else {{\n                Some({})\n            }}",
                null_condition,
                generate_null_check(use_flexible),
                generate_inner_decode_body(field),
                generate_inner_decode_body(field)
            );
        }
        return format!("Some({})", generate_inner_decode_body(field));
    }

    generate_inner_decode_body(field)
}

/// 生成 null 检查表达式
fn generate_null_check(use_flexible: bool) -> String {
    if use_flexible {
        "let len = decode_unsigned_varint(buf); len == 0".to_string()
    } else {
        "let peek = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]); peek == -1".to_string()
    }
}

/// 生成内部类型解码体
fn generate_inner_decode_body(field: &ParsedField) -> String {
    let rust_type = &field.rust_type;
    let use_flexible = is_flexible_field(field);

    // 移除 Option 包装
    let inner_type = if rust_type.starts_with("Option<") {
        &rust_type[7..rust_type.len() - 1]
    } else {
        rust_type
    };

    match inner_type {
        "String" => {
            if use_flexible {
                "decode_compact_string(buf)?".to_string()
            } else {
                "decode_string(buf)?".to_string()
            }
        }
        "Bytes" => {
            if use_flexible {
                "decode_compact_bytes(buf)?".to_string()
            } else {
                "decode_bytes(buf)?".to_string()
            }
        }
        s if s.starts_with("Vec<") => {
            if use_flexible {
                "decode_compact_array(buf, |b| <_>::decode(b, version))?".to_string()
            } else {
                "decode_array(buf, |b| <_>::decode(b, version))?".to_string()
            }
        }
        "bool" => "buf.get_i8() != 0".to_string(),
        "i8" => "buf.get_i8()".to_string(),
        "i16" => "buf.get_i16()".to_string(),
        "i32" => "buf.get_i32()".to_string(),
        "i64" => "buf.get_i64()".to_string(),
        "u8" => "buf.get_u8()".to_string(),
        "u16" => "buf.get_u16()".to_string(),
        "u32" => "buf.get_u32()".to_string(),
        "u64" => "buf.get_u64()".to_string(),
        "f64" => "buf.get_f64()".to_string(),
        "Uuid" => "Uuid::decode(buf)?".to_string(),
        _ => format!("<{} as Message>::decode(buf, version)?", inner_type),
    }
}

/// 生成 size 字段代码
fn generate_size_field(content: &mut String, field: &ParsedField) {
    let condition = generate_version_condition(&field.versions);
    let field_name = &field.rust_name;
    let rust_type = &field.rust_type;

    if field.versions != "0+" && field.versions != "all" {
        content.push_str(&format!("        if {} {{\n", condition));
        content.push_str(&format!(
            "            total += self.{}.size(version);\n",
            field_name
        ));
        content.push_str("        }\n");
    } else {
        content.push_str(&format!(
            "        total += self.{}.size(version);\n",
            field_name
        ));
    }
}

/// 生成版本条件表达式
fn generate_version_condition(versions: &str) -> String {
    if versions.contains('-') {
        let parts: Vec<&str> = versions.split('-').collect();
        format!("version >= {} && version <= {}", parts[0], parts[1])
    } else if versions.ends_with('+') {
        let start = versions.trim_end_matches('+');
        format!("version >= {}", start)
    } else if versions == "0+" || versions == "all" {
        "true".to_string()
    } else {
        format!("version == {}", versions)
    }
}

/// 判断字段是否使用灵活格式
fn is_flexible_field(field: &ParsedField) -> bool {
    field.tagged_versions.is_some() || field.versions == "9+"
}

/// 解析最小版本
fn parse_min_version(versions: &str) -> i16 {
    if versions.contains('-') {
        versions.split('-').next().unwrap().parse().unwrap_or(0)
    } else if versions.ends_with('+') {
        versions.trim_end_matches('+').parse().unwrap_or(0)
    } else if versions == "0+" || versions == "all" {
        0
    } else {
        versions.parse().unwrap_or(0)
    }
}

/// 解析最大版本
fn parse_max_version(versions: &str) -> i16 {
    if versions.contains('-') {
        versions
            .split('-')
            .nth(1)
            .unwrap()
            .parse()
            .unwrap_or(i16::MAX)
    } else if versions.ends_with('+') {
        i16::MAX
    } else if versions == "0+" || versions == "all" {
        i16::MAX
    } else {
        versions.parse().unwrap_or(0)
    }
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
        content.push_str(&format!("mod {};\n", file_name));
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

    // 生成静态数组
    content.push_str("/// 客户端支持的 API 版本范围（静态数组）\n");
    content.push_str("pub const CLIENT_SUPPORTED_VERSIONS: &[(i16, i16, i16)] = &[\n");

    for msg in messages {
        if let Some(api_key) = msg.api_key {
            let (min, max) = parse_version_range(&msg.valid_versions);
            content.push_str(&format!(
                "    ({}, {}, {}), // {}\n",
                api_key, min, max, msg.struct_name
            ));
        }
    }

    content.push_str("];\n\n");

    // 生成辅助函数
    content.push_str("/// 获取指定 API 的支持版本范围\n");
    content.push_str("pub fn get_version_range(api_key: i16) -> Option<(i16, i16)> {\n");
    content.push_str("    CLIENT_SUPPORTED_VERSIONS\n");
    content.push_str("        .iter()\n");
    content.push_str("        .find(|&&(key, _, _)| key == api_key)\n");
    content.push_str("        .map(|&(_, min, max)| (min, max))\n");
    content.push_str("}\n\n");

    content.push_str("/// 检查是否支持指定版本\n");
    content.push_str("pub fn supports_version(api_key: i16, version: i16) -> bool {\n");
    content.push_str("    CLIENT_SUPPORTED_VERSIONS\n");
    content.push_str("        .iter()\n");
    content.push_str("        .find(|&&(key, _, _)| key == api_key)\n");
    content.push_str("        .map_or(false, |&(_, min, max)| version >= min && version <= max)\n");
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
