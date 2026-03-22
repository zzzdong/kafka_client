//! 使用 Derive 宏生成 Kafka API 协议代码
//!
//! 从协议定义 JSON 文件生成使用 #[derive(KafkaMessage)] 的 Rust 代码
//!
//! 使用方法:
//!   cargo run --bin generate_api_derive -- [options]

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut input_dir = PathBuf::from("protocol_definitions");
    let mut output_dir = PathBuf::from("src/protocol/api");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-i" | "--input" => {
                if i + 1 < args.len() {
                    input_dir = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --input requires a value");
                    std::process::exit(1);
                }
            }
            "-o" | "--output" => {
                if i + 1 < args.len() {
                    output_dir = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --output requires a value");
                    std::process::exit(1);
                }
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
    }

    println!("Generating Kafka API protocol code with derive macros...");
    println!("Input: {}", input_dir.display());
    println!("Output: {}", output_dir.display());
    println!();

    if let Err(e) = generate_api_code(&input_dir, &output_dir) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    println!("\nDone!");
}

fn print_usage() {
    println!("Usage: generate_api_derive [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -i, --input <PATH>   Input directory with protocol JSON files (default: protocol_definitions)");
    println!("  -o, --output <PATH>  Output directory for generated code (default: src/protocol/api_derive)");
    println!("  -h, --help           Print this help message");
}

fn generate_api_code(input_dir: &PathBuf, output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(output_dir)?;

    // 收集所有 API 定义（按 API 名称分组）
    let mut api_defs: HashMap<String, (Option<ApiDefinition>, Option<ApiDefinition>)> = HashMap::new();

    for entry in std::fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let file_name = path.file_stem().unwrap().to_string_lossy().to_string();
            let content = std::fs::read_to_string(&path)?;

            // 预处理：移除 JSON 注释
            let cleaned_content = remove_json_comments(&content);

            // 解析 JSON
            let def: ApiDefinition = match serde_json::from_str(&cleaned_content) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Warning: Failed to parse {}: {}", file_name, e);
                    continue;
                }
            };

            // 从文件名解析 API 名称和类型
            if let Some((api_name, is_request)) = parse_api_name(&file_name) {
                let entry = api_defs.entry(api_name).or_insert((None, None));
                if is_request {
                    entry.0 = Some(def);
                } else {
                    entry.1 = Some(def);
                }
            }
        }
    }

    // 为每个 API 生成代码
    let mut mod_list = Vec::new();

    for (name, (request_def, response_def)) in api_defs {
        let module_name = to_snake_case(&name);
        let file_path = output_dir.join(format!("{}.rs", module_name));

        generate_api_file(&file_path, &name, request_def, response_def)?;
        mod_list.push(module_name);
    }

    // 生成 mod.rs
    generate_mod_file(&output_dir.join("mod.rs"), &mod_list)?;

    Ok(())
}

fn parse_api_name(file_name: &str) -> Option<(String, bool)> {
    // 文件名格式: ApiNameRequest.json 或 ApiNameResponse.json
    if file_name.ends_with("Request") {
        let name = &file_name[..file_name.len() - 7];
        Some((name.to_string(), true))
    } else if file_name.ends_with("Response") {
        let name = &file_name[..file_name.len() - 8];
        Some((name.to_string(), false))
    } else {
        None
    }
}

/// 递归收集所有字段（包括嵌套字段）
fn collect_all_fields<'a>(fields: &'a [FieldDef], result: &mut Vec<&'a FieldDef>) {
    for field in fields {
        result.push(field);
        if let Some(ref nested) = field.fields {
            collect_all_fields(nested, result);
        }
    }
}

/// 检查字段是否需要 Uuid 类型
fn field_needs_uuid(field: &FieldDef) -> bool {
    if field.ty == "uuid" || field.item_type.as_deref() == Some("uuid") {
        return true;
    }
    if let Some(ref nested) = field.fields {
        return nested.iter().any(|f| field_needs_uuid(f));
    }
    false
}

fn generate_api_file(
    file_path: &PathBuf,
    name: &str,
    request_def: Option<ApiDefinition>,
    response_def: Option<ApiDefinition>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(file_path)?;
    let mut content = String::new();

    // 文件头
    content.push_str(&format!("//! {} API\n", name));
    content.push_str("//!\n");
    content.push_str("//! 使用 derive 宏自动生成\n");
    content.push_str("//!\n");

    if let Some(ref def) = request_def {
        if let Some(api_key) = def.api_key {
            content.push_str(&format!("//! API Key: {}\n", api_key));
        }
    }

    content.push_str("\n");

    // 收集所有字段用于检测需要的导入
    let mut all_fields: Vec<&FieldDef> = Vec::new();
    if let Some(ref def) = request_def {
        if let Some(ref fields) = def.fields {
            collect_all_fields(fields, &mut all_fields);
        }
    }
    if let Some(ref def) = response_def {
        if let Some(ref fields) = def.fields {
            collect_all_fields(fields, &mut all_fields);
        }
    }

    // 检测是否需要 Uuid
    let needs_uuid = all_fields.iter().any(|f| field_needs_uuid(f));

    // 导入
    content.push_str("use bytes::{Bytes, BytesMut};\n");
    content.push_str("use crate::protocol::{\n");
    content.push_str("    Message, RequestMessage, ResponseMessage, ProtocolResult,\n");
    content.push_str("    KafkaMessage, KafkaRequest, KafkaResponse,\n");
    content.push_str("    RequestHeader,\n");
    if needs_uuid {
        content.push_str("    Uuid,\n");
    }
    content.push_str("};\n");
    content.push_str("\n");

    // 生成请求结构体
    if let Some(def) = request_def {
        let struct_name = format!("{}Request", name);
        let fields = def.fields.as_ref().map(|f| f.as_slice()).unwrap_or(&[]);
        let prefix = format!("{}Request", name);

        let api_key = def.api_key.unwrap_or(0);
        let request_struct = generate_struct_with_derive(&struct_name, fields, api_key, &def.valid_versions, &def.flexible_versions, true, &prefix);
        content.push_str(&request_struct);
        content.push('\n');

        // 生成请求嵌套结构体（带前缀）
        for field in fields {
            if let Some(ref nested_fields) = field.fields {
                let nested_name = if field.ty.starts_with("[]") {
                    &field.ty[2..]
                } else {
                    &field.ty
                };
                content.push('\n');
                content.push_str(&generate_nested_struct_with_prefix(nested_name, nested_fields, &prefix));
            }
        }
        
        // 生成 commonStructs（带前缀）
        if let Some(ref common_structs) = def.common_structs {
            for common_struct in common_structs {
                content.push('\n');
                content.push_str(&generate_common_struct_with_prefix(&common_struct.name, &common_struct.fields, &prefix));
            }
        }
    }

    // 生成响应结构体
    if let Some(def) = response_def {
        let struct_name = format!("{}Response", name);
        let fields = def.fields.as_ref().map(|f| f.as_slice()).unwrap_or(&[]);
        let prefix = format!("{}Response", name);

        let api_key = def.api_key.unwrap_or(0);
        let response_struct = generate_struct_with_derive(&struct_name, fields, api_key, &def.valid_versions, &def.flexible_versions, false, &prefix);
        content.push_str(&response_struct);
        content.push('\n');

        // 生成响应嵌套结构体（带前缀）
        for field in fields {
            if let Some(ref nested_fields) = field.fields {
                let nested_name = if field.ty.starts_with("[]") {
                    &field.ty[2..]
                } else {
                    &field.ty
                };
                content.push('\n');
                content.push_str(&generate_nested_struct_with_prefix(nested_name, nested_fields, &prefix));
            }
        }
        
        // 生成 commonStructs（带前缀）
        if let Some(ref common_structs) = def.common_structs {
            for common_struct in common_structs {
                content.push('\n');
                content.push_str(&generate_common_struct_with_prefix(&common_struct.name, &common_struct.fields, &prefix));
            }
        }
    }

    file.write_all(content.as_bytes())?;

    Ok(())
}

fn generate_struct_with_derive(
    name: &str,
    fields: &[FieldDef],
    api_key: i16,
    valid_versions: &str,
    flexible_versions: &Option<String>,
    is_request: bool,
    prefix: &str,
) -> String {
    let mut content = String::new();

    content.push_str(&format!("/// {}\n", name));

    // 添加 derive 宏
    content.push_str("#[derive(Debug, Clone, Default, PartialEq, KafkaMessage");
    if is_request {
        content.push_str(", KafkaRequest");
    } else {
        content.push_str(", KafkaResponse");
    }
    content.push_str(")]\n");

    // 添加 kafka 属性
    let mut kafka_attrs = format!("api_key = {}, valid_versions = \"{}\"", api_key, valid_versions);
    if let Some(flex) = flexible_versions {
        kafka_attrs.push_str(&format!(", flexible_versions = \"{}\"", flex));
    }
    content.push_str(&format!("#[kafka({})]\n", kafka_attrs));

    content.push_str(&format!("pub struct {} {{\n", name));

    for field in fields {
        // 添加文档注释
        if let Some(ref doc) = field.doc {
            content.push_str(&format!("    /// {}\n", doc));
        }

        let field_name = to_snake_case(&field.name);
        let rust_type = get_rust_type_with_prefix(field, prefix);

        // 构建 kafka 字段属性
        let mut field_attrs = Vec::new();

        if let Some(ref versions) = field.versions {
            field_attrs.push(format!("versions = \"{}\"", versions));
        }

        if field.nullable == Some(true) {
            field_attrs.push("nullable".to_string());
        }

        if field.flexible == Some(true) {
            field_attrs.push("flexible".to_string());
        }

        if !field_attrs.is_empty() {
            content.push_str(&format!("    #[kafka({})]\n", field_attrs.join(", ")));
        }

        // 确定字段类型 - 只有明确标记为 nullable 的字段才使用 Option
        let is_optional = field.nullable == Some(true);

        if is_optional {
            content.push_str(&format!("    pub {}: Option<{}>,\n", field_name, rust_type));
        } else {
            content.push_str(&format!("    pub {}: {},\n", field_name, rust_type));
        }
    }

    content.push_str("}\n");

    content
}

fn generate_nested_struct(name: &str, fields: &[FieldDef]) -> String {
    let mut content = String::new();

    content.push_str(&format!("/// {}\n", name));
    content.push_str("#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]\n");
    content.push_str(&format!("pub struct {} {{\n", name));

    for field in fields {
        if let Some(ref doc) = field.doc {
            content.push_str(&format!("    /// {}\n", doc));
        }

        let field_name = to_snake_case(&field.name);
        let rust_type = get_rust_type(field);

        let mut field_attrs = Vec::new();

        if let Some(ref versions) = field.versions {
            field_attrs.push(format!("versions = \"{}\"", versions));
        }

        if field.nullable == Some(true) {
            field_attrs.push("nullable".to_string());
        }

        if !field_attrs.is_empty() {
            content.push_str(&format!("    #[kafka({})]\n", field_attrs.join(", ")));
        }

        // 只有明确标记为 nullable 的字段才使用 Option
        let is_optional = field.nullable == Some(true);

        if is_optional {
            content.push_str(&format!("    pub {}: Option<{}>,\n", field_name, rust_type));
        } else {
            content.push_str(&format!("    pub {}: {},\n", field_name, rust_type));
        }
    }

    content.push_str("}\n");

    // 递归生成更深层的嵌套结构体
    for field in fields {
        if let Some(ref nested_fields) = field.fields {
            let struct_name = if field.ty.starts_with("[]") {
                &field.ty[2..]
            } else {
                &field.ty
            };
            content.push('\n');
            content.push_str(&generate_nested_struct(struct_name, nested_fields));
        }
    }

    content
}

fn generate_mod_file(file_path: &PathBuf, modules: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(file_path)?;
    let mut content = String::new();

    content.push_str("//! Kafka API 协议定义 (使用 Derive 宏)\n");
    content.push_str("//!\n");
    content.push_str("//! 本模块使用 #[derive(KafkaMessage)] 自动生成\n\n");

    for module in modules {
        content.push_str(&format!("pub mod {};\n", module));
    }

    content.push('\n');

    // 重新导出所有请求和响应类型
    for module in modules {
        let camel_name = to_camel_case(module);
        content.push_str(&format!("pub use {}::{}Request;\n", module, camel_name));
        content.push_str(&format!("pub use {}::{}Response;\n", module, camel_name));
    }

    file.write_all(content.as_bytes())?;
    Ok(())
}

/// 生成带前缀的嵌套结构体
fn generate_nested_struct_with_prefix(name: &str, fields: &[FieldDef], prefix: &str) -> String {
    let full_name = format!("{}{}", prefix, name);
    let mut content = String::new();

    content.push_str(&format!("/// {}\n", full_name));
    content.push_str("#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]\n");
    content.push_str(&format!("pub struct {} {{\n", full_name));

    for field in fields {
        if let Some(ref doc) = field.doc {
            content.push_str(&format!("    /// {}\n", doc));
        }

        let field_name = to_snake_case(&field.name);
        let rust_type = get_rust_type_with_prefix(field, prefix);

        let mut field_attrs = Vec::new();

        if let Some(ref versions) = field.versions {
            field_attrs.push(format!("versions = \"{}\"", versions));
        }

        if field.nullable == Some(true) {
            field_attrs.push("nullable".to_string());
        }

        if !field_attrs.is_empty() {
            content.push_str(&format!("    #[kafka({})]\n", field_attrs.join(", ")));
        }

        // 只有明确标记为 nullable 的字段才使用 Option
        let is_optional = field.nullable == Some(true);

        if is_optional {
            content.push_str(&format!("    pub {}: Option<{}>,\n", field_name, rust_type));
        } else {
            content.push_str(&format!("    pub {}: {},\n", field_name, rust_type));
        }
    }

    content.push_str("}\n");

    // 递归生成更深层的嵌套结构体（保持相同前缀）
    for field in fields {
        if let Some(ref nested_fields) = field.fields {
            let struct_name = if field.ty.starts_with("[]") {
                &field.ty[2..]
            } else {
                &field.ty
            };
            content.push('\n');
            content.push_str(&generate_nested_struct_with_prefix(struct_name, nested_fields, prefix));
        }
    }

    content
}

/// 生成带前缀的 common struct
fn generate_common_struct_with_prefix(name: &str, fields: &[FieldDef], prefix: &str) -> String {
    let full_name = format!("{}{}", prefix, name);
    let mut content = String::new();

    content.push_str(&format!("/// {}\n", full_name));
    content.push_str("#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]\n");
    content.push_str(&format!("pub struct {} {{\n", full_name));

    for field in fields {
        if let Some(ref doc) = field.doc {
            content.push_str(&format!("    /// {}\n", doc));
        }

        let field_name = to_snake_case(&field.name);
        // common struct 中的字段引用的类型也应该带前缀
        let rust_type = get_common_struct_field_type(&field.ty, prefix);

        let mut field_attrs = Vec::new();

        if let Some(ref versions) = field.versions {
            field_attrs.push(format!("versions = \"{}\"", versions));
        }

        if field.nullable == Some(true) {
            field_attrs.push("nullable".to_string());
        }

        if !field_attrs.is_empty() {
            content.push_str(&format!("    #[kafka({})]\n", field_attrs.join(", ")));
        }

        // 只有明确标记为 nullable 的字段才使用 Option
        let is_optional = field.nullable == Some(true);

        if is_optional {
            content.push_str(&format!("    pub {}: Option<{}>,\n", field_name, rust_type));
        } else {
            content.push_str(&format!("    pub {}: {},\n", field_name, rust_type));
        }
    }

    content.push_str("}\n");

    content
}

/// 获取 common struct 字段的类型（为嵌套类型添加前缀）
fn get_common_struct_field_type(ty: &str, prefix: &str) -> String {
    // 如果类型是数组
    if ty.starts_with("[]") {
        let inner = &ty[2..];
        // 为基本类型（非原始类型）添加前缀
        if is_nested_type(inner) {
            return format!("Vec<{}{}>", prefix, inner);
        }
        return format!("Vec<{}>", get_rust_type_for_common_struct(inner));
    }
    
    // 为基本类型（非原始类型）添加前缀
    if is_nested_type(ty) {
        return format!("{}{}", prefix, ty);
    }
    
    get_rust_type_for_common_struct(ty)
}

/// 将 Kafka 类型映射为 Rust 类型（用于 common struct）
fn get_rust_type_for_common_struct(ty: &str) -> String {
    match ty {
        "string" => "String".to_string(),
        "int8" => "i8".to_string(),
        "int16" => "i16".to_string(),
        "int32" => "i32".to_string(),
        "int64" => "i64".to_string(),
        "bool" => "bool".to_string(),
        "uuid" => "Uuid".to_string(),
        "bytes" => "Vec<u8>".to_string(),
        "float64" => "f64".to_string(),
        _ => ty.to_string(),
    }
}

/// 检查类型是否是嵌套结构体（非原始类型）
fn is_nested_type(name: &str) -> bool {
    // 原始类型列表
    let primitive_types = ["string", "int8", "int16", "int32", "int64", "bool", "uuid", "bytes", "float64"];
    !primitive_types.contains(&name.to_lowercase().as_str())
}

/// 获取带前缀的 Rust 类型
fn get_rust_type_with_prefix(field: &FieldDef, prefix: &str) -> String {
    let base_type = get_rust_type(field);
    
    // 如果字段有嵌套定义，说明这是一个嵌套结构体，需要添加前缀
    if field.fields.is_some() {
        // 如果类型是数组，为内部类型添加前缀
        if base_type.starts_with("Vec<") && base_type.ends_with(">") {
            let inner = &base_type[4..base_type.len()-1];
            return format!("Vec<{}{}>", prefix, inner);
        }
        
        // 为基本类型添加前缀
        return format!("{}{}", prefix, base_type);
    }
    
    base_type
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_lowercase = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && prev_lowercase {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
            prev_lowercase = false;
        } else {
            result.push(c);
            prev_lowercase = true;
        }
    }

    result
}

fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '_' || c == '-' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_uppercase().next().unwrap());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

fn get_rust_type(field: &FieldDef) -> String {
    // 检查类型是否以 [] 开头表示数组
    if field.ty.starts_with("[]") {
        let item_type = &field.ty[2..]; // 去掉 []
        format!("Vec<{}>", map_kafka_type(item_type))
    } else if field.is_array == Some(true) {
        let item_type = field.item_type.as_deref().unwrap_or("i32");
        format!("Vec<{}>", map_kafka_type(item_type))
    } else if field.is_map == Some(true) {
        let key_type = field.map_key_type.as_deref().unwrap_or("string");
        let value_type = field.map_value_type.as_deref().unwrap_or("bytes");
        format!(
            "std::collections::HashMap<{}, {}>",
            map_kafka_type(key_type),
            map_kafka_type(value_type)
        )
    } else {
        map_kafka_type(&field.ty)
    }
}

fn map_kafka_type(kafka_type: &str) -> String {
    match kafka_type {
        "bool" => "bool".to_string(),
        "int8" | "i8" => "i8".to_string(),
        "int16" | "i16" | "uint16" => "i16".to_string(),
        "int32" | "i32" | "uint32" => "i32".to_string(),
        "int64" | "i64" | "uint64" => "i64".to_string(),
        "string" => "String".to_string(),
        "bytes" => "Vec<u8>".to_string(),
        "records" => "Vec<u8>".to_string(),
        "uuid" => "Uuid".to_string(),
        other => to_camel_case(other),
    }
}

/// 移除 JSON 文件中的注释（// 和 /* */）
fn remove_json_comments(content: &str) -> String {
    let mut result = String::new();
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '/' {
            if let Some(&next) = chars.peek() {
                if next == '/' {
                    // 单行注释，跳过到行尾
                    chars.next();
                    while let Some(c) = chars.next() {
                        if c == '\n' {
                            result.push('\n');
                            break;
                        }
                    }
                } else if next == '*' {
                    // 多行注释，跳过到 */
                    chars.next();
                    let mut prev = ' ';
                    while let Some(c) = chars.next() {
                        if prev == '*' && c == '/' {
                            break;
                        }
                        prev = c;
                    }
                } else {
                    result.push(c);
                }
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }

    result
}

// JSON 数据结构
#[derive(Debug, serde::Deserialize)]
struct ApiDefinition {
    #[serde(rename = "apiKey", default)]
    api_key: Option<i16>,
    #[serde(rename = "type")]
    type_: String,
    name: String,
    #[serde(rename = "validVersions")]
    valid_versions: String,
    #[serde(rename = "flexibleVersions", default)]
    flexible_versions: Option<String>,
    fields: Option<Vec<FieldDef>>,
    #[serde(rename = "commonStructs", default)]
    common_structs: Option<Vec<CommonStructDef>>,
}

#[derive(Debug, serde::Deserialize)]
struct CommonStructDef {
    name: String,
    versions: String,
    fields: Vec<FieldDef>,
}

impl ApiDefinition {
    fn min_version(&self) -> i16 {
        parse_version_range(&self.valid_versions).0.unwrap_or(0)
    }

    fn max_version(&self) -> i16 {
        parse_version_range(&self.valid_versions).1.unwrap_or(0)
    }
}

#[derive(Debug, serde::Deserialize)]
struct FieldDef {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    versions: Option<String>,
    #[serde(default)]
    nullable: Option<bool>,
    #[serde(default)]
    flexible: Option<bool>,
    #[serde(rename = "default", default)]
    default_value: Option<serde_json::Value>,
    about: Option<String>,
    doc: Option<String>,
    #[serde(rename = "fields", default)]
    fields: Option<Vec<FieldDef>>,
    #[serde(rename = "isArray", default)]
    is_array: Option<bool>,
    #[serde(rename = "itemType", default)]
    item_type: Option<String>,
    #[serde(rename = "isMap", default)]
    is_map: Option<bool>,
    #[serde(rename = "mapKeyType", default)]
    map_key_type: Option<String>,
    #[serde(rename = "mapValueType", default)]
    map_value_type: Option<String>,
}

fn parse_version_range(range: &str) -> (Option<i16>, Option<i16>) {
    if range.ends_with('+') {
        let min = range.trim_end_matches('+').parse().ok();
        (min, None)
    } else if range.contains('-') {
        let parts: Vec<_> = range.split('-').collect();
        let min = parts[0].parse().ok();
        let max = parts.get(1).and_then(|s| s.parse().ok());
        (min, max)
    } else {
        (range.parse().ok(), None)
    }
}
