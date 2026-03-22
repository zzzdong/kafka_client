//! 生成 Kafka API 协议代码
//!
//! 从协议定义 JSON 文件生成 Rust 代码
//!
//! 使用方法:
//!   cargo run --bin generate_api -- [options]
//!
//! 选项:
//!   -i, --input <PATH>   协议定义目录 (默认: protocol_definitions)
//!   -o, --output <PATH>  输出目录 (默认: src/protocol/api)

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

    println!("Generating Kafka API protocol code...");
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
    println!("Usage: generate_api [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -i, --input <PATH>   Input directory with protocol JSON files (default: protocol_definitions)");
    println!("  -o, --output <PATH>  Output directory for generated code (default: src/protocol/api)");
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

            // 提取 API 名称（去掉 Request/Response 后缀）
            let api_name = if file_name.ends_with("Request") {
                file_name.trim_end_matches("Request").to_string()
            } else if file_name.ends_with("Response") {
                file_name.trim_end_matches("Response").to_string()
            } else {
                file_name.clone()
            };

            let entry = api_defs.entry(api_name).or_insert((None, None));
            if file_name.ends_with("Request") {
                entry.0 = Some(def);
            } else if file_name.ends_with("Response") {
                entry.1 = Some(def);
            }
        }
    }

    // 生成每个 API 的代码
    let mut api_modules: Vec<(String, i16)> = Vec::new();

    for (api_name, (request_def, response_def)) in &api_defs {
        let api_key = request_def.as_ref()
            .or(response_def.as_ref())
            .map(|d| d.api_key)
            .unwrap_or(-1);

        match generate_api_module(api_name, request_def, response_def, output_dir) {
            Ok(_) => {
                let module_name = to_snake_case(api_name);
                api_modules.push((module_name, api_key));
                println!("Generated: {}.rs (API Key: {})", api_name, api_key);
            }
            Err(e) => {
                eprintln!("Failed to generate {}: {}", api_name, e);
            }
        }
    }

    // 按 API Key 排序
    api_modules.sort_by_key(|(_, api_key)| *api_key);

    // 生成 mod.rs
    generate_mod_rs(&api_modules, output_dir)?;
    println!("Generated: mod.rs");

    Ok(())
}

fn generate_api_module(
    name: &str,
    request_def: &Option<ApiDefinition>,
    response_def: &Option<ApiDefinition>,
    output_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let module_name = to_snake_case(name);
    let file_path = output_dir.join(format!("{}.rs", module_name));
    let mut file = File::create(&file_path)?;

    let mut content = String::new();

    content.push_str(&format!("//! {} API\n", name));
    content.push_str("//!\n");
    content.push_str(&format!("//! 自动生成于 Kafka 协议定义\n"));
    content.push_str("//!\n");
    if let Some(def) = request_def {
        content.push_str(&format!("//! API Key: {}\n", def.api_key));
    }
    content.push_str("\n");

    content.push_str("use bytes::{Buf, BufMut, Bytes, BytesMut};\n");
    content.push_str("use crate::protocol::{\n");
    content.push_str("    Message, RequestMessage, ResponseMessage, ProtocolResult, ProtocolError,\n");
    content.push_str("    encode_string, encode_nullable_string, encode_array,\n");
    content.push_str("    decode_string, decode_nullable_string, decode_array,\n");
    content.push_str("    encode_compact_string, encode_compact_nullable_string, encode_compact_array,\n");
    content.push_str("    decode_compact_string, decode_compact_nullable_string, decode_compact_array,\n");
    content.push_str("    encode_compact_bytes, decode_compact_bytes,\n");
    content.push_str("    encode_compact_nullable_bytes, decode_compact_nullable_bytes,\n");
    content.push_str("    encode_unsigned_varint, decode_unsigned_varint,\n");
    content.push_str("    encode_uuid, decode_uuid,\n");
    content.push_str("    encode_records, decode_records,\n");
    content.push_str("    skip_tagged_fields, encode_tagged_fields,\n");
    content.push_str("    RequestHeader,\n");
    content.push_str("};\n\n");
    content.push_str("use uuid::Uuid;\n\n");

    // 生成请求结构体和实现
    if let Some(def) = request_def {
        let struct_name = format!("{}Request", name);
        let fields = def.fields.as_ref().map(|f| f.as_slice()).unwrap_or(&[]);

        let request_struct = generate_struct(&struct_name, fields, true);
        content.push_str(&request_struct);
        content.push('\n');

        let request_impl = generate_request_impl(
            name,
            def.api_key,
            fields,
            def.min_version(),
            def.max_version(),
        );
        content.push_str(&request_impl);
        content.push('\n');
    }

    // 生成响应结构体和实现
    if let Some(def) = response_def {
        let struct_name = format!("{}Response", name);
        let fields = def.fields.as_ref().map(|f| f.as_slice()).unwrap_or(&[]);

        let response_struct = generate_struct(&struct_name, fields, false);
        content.push_str(&response_struct);
        content.push('\n');

        let response_impl = generate_response_impl(
            name,
            fields,
            def.min_version(),
            def.max_version(),
        );
        content.push_str(&response_impl);
    }

    file.write_all(content.as_bytes())?;

    Ok(())
}

fn generate_struct(name: &str, fields: &[FieldDef], _is_request: bool) -> String {
    let mut content = String::new();

    content.push_str(&format!("/// {}\n", name));
    content.push_str("#[derive(Debug, Clone, Default, PartialEq)]\n");
    content.push_str(&format!("pub struct {} {{\n", name));

    for field in fields {
        if let Some(ref doc) = field.doc {
            content.push_str(&format!("    /// {}\n", doc));
        }

        let field_name = to_snake_case(&field.name);
        let rust_type = get_rust_type(field);

        let is_optional = field.nullable == Some(true) || field.versions.is_some();

        if is_optional {
            content.push_str(&format!("    pub {}: Option<{}>,\n", field_name, rust_type));
        } else {
            content.push_str(&format!("    pub {}: {},\n", field_name, rust_type));
        }
    }

    content.push_str("}\n");

    // 递归生成嵌套结构体（用于数组元素）
    for field in fields {
        if let Some(ref nested_fields) = field.fields {
            // 从类型名称中提取结构体名称（去掉 [] 前缀）
            let struct_name = if field.ty.starts_with("[]") {
                &field.ty[2..]
            } else {
                &field.ty
            };
            content.push('\n');
            content.push_str(&generate_struct(struct_name, nested_fields, _is_request));
            content.push('\n');
            // 为嵌套结构体生成 Message trait 实现
            content.push_str(&generate_nested_message_impl(struct_name, nested_fields));
            content.push('\n');
        }
    }

    content
}

fn generate_request_impl(
    name: &str,
    api_key: i16,
    fields: &[FieldDef],
    min_version: i16,
    _max_version: i16,
) -> String {
    let mut content = String::new();
    let struct_name = format!("{}Request", name);

    content.push_str(&format!("impl Message for {} {{\n", struct_name));

    content.push_str("    fn type_name() -> &'static str {\n");
    content.push_str(&format!("        \"{}\"\n", struct_name));
    content.push_str("    }\n\n");

    content.push_str("    fn api_key(&self) -> Option<i16> {\n");
    content.push_str(&format!("        Some({})\n", api_key));
    content.push_str("    }\n\n");

    content.push_str("    fn default_version() -> i16 {\n");
    content.push_str(&format!("        {}\n", min_version));
    content.push_str("    }\n\n");

    content.push_str("    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {\n");
    content.push_str(&generate_encode_body(fields));
    content.push_str("    }\n\n");

    content.push_str("    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {\n");
    content.push_str(&generate_decode_body(fields));
    content.push_str("    }\n");

    content.push_str("}\n\n");

    content.push_str(&format!("impl RequestMessage for {} {{\n", struct_name));
    content.push_str("    fn request_header(&self, version: i16, correlation_id: i32, client_id: &str) -> RequestHeader {\n");
    content.push_str("        RequestHeader::new_v1(\n");
    content.push_str(&format!("            {}, // api_key\n", api_key));
    content.push_str("            version,\n");
    content.push_str("            correlation_id,\n");
    content.push_str("            if client_id.is_empty() { None } else { Some(client_id.to_string()) },\n");
    content.push_str("        )\n");
    content.push_str("    }\n");
    content.push_str("}\n");

    content
}

fn generate_response_impl(
    name: &str,
    fields: &[FieldDef],
    min_version: i16,
    _max_version: i16,
) -> String {
    let mut content = String::new();
    let struct_name = format!("{}Response", name);

    content.push_str(&format!("impl Message for {} {{\n", struct_name));

    content.push_str("    fn type_name() -> &'static str {\n");
    content.push_str(&format!("        \"{}\"\n", struct_name));
    content.push_str("    }\n\n");

    content.push_str("    fn api_key(&self) -> Option<i16> {\n");
    content.push_str("        None\n");
    content.push_str("    }\n\n");

    content.push_str("    fn default_version() -> i16 {\n");
    content.push_str(&format!("        {}\n", min_version));
    content.push_str("    }\n\n");

    content.push_str("    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {\n");
    content.push_str(&generate_encode_body(fields));
    content.push_str("    }\n\n");

    content.push_str("    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {\n");
    content.push_str(&generate_decode_body(fields));
    content.push_str("    }\n");

    content.push_str("}\n\n");

    content.push_str(&format!("impl ResponseMessage for {} {{}}\n", struct_name));

    content
}

/// 为嵌套结构体生成 Message trait 实现
fn generate_nested_message_impl(name: &str, fields: &[FieldDef]) -> String {
    let mut content = String::new();

    content.push_str(&format!("impl Message for {} {{\n", name));

    content.push_str("    fn type_name() -> &'static str {\n");
    content.push_str(&format!("        \"{}\"\n", name));
    content.push_str("    }\n\n");

    content.push_str("    fn api_key(&self) -> Option<i16> {\n");
    content.push_str("        None\n");
    content.push_str("    }\n\n");

    content.push_str("    fn default_version() -> i16 {\n");
    content.push_str("        0\n");
    content.push_str("    }\n\n");

    content.push_str("    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {\n");
    content.push_str(&generate_encode_body(fields));
    content.push_str("    }\n\n");

    content.push_str("    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {\n");
    content.push_str(&generate_decode_body(fields));
    content.push_str("    }\n");

    content.push_str("}\n");

    content
}

fn generate_encode_body(fields: &[FieldDef]) -> String {
    let mut content = String::new();

    for field in fields {
        let field_name = to_snake_case(&field.name);

        if let Some(ref versions) = field.versions {
            let (min_ver, max_ver) = parse_version_range(versions);
            if let Some(min) = min_ver {
                content.push_str(&format!("        if version >= {} {{\n", min));
                if let Some(max) = max_ver {
                    content.push_str(&format!("            if version <= {} {{\n", max));
                    content.push_str(&generate_field_encode(field, &field_name));
                    content.push_str("            }\n");
                } else {
                    content.push_str(&generate_field_encode(field, &field_name));
                }
                content.push_str("        }\n");
            }
        } else {
            content.push_str(&generate_field_encode(field, &field_name));
        }
    }

    content.push_str("        Ok(())\n");

    content
}

fn generate_field_encode(field: &FieldDef, field_name: &str) -> String {
    let mut content = String::new();

    if field.tagged == Some(true) && field.tag.is_some() {
        let tag = field.tag.unwrap();
        content.push_str(&format!("        // Tagged field: {} (tag={})\n", field_name, tag));
        return content;
    }

    // 检查类型是否以 [] 开头表示数组
    let ty = &field.ty;
    if ty.starts_with("[]") {
        let item_type = &ty[2..]; // 去掉 []
        if is_primitive_type(item_type) {
            content.push_str(&format!("        encode_array(buf, &self.{}, |b, item| {{\n", field_name));
            content.push_str(&format!("            {}\n", get_encode_primitive(item_type, "item")));
            content.push_str("        });\n");
        } else {
            content.push_str(&format!("        encode_array(buf, &self.{}, |b, item| {{\n", field_name));
            content.push_str("            item.encode(b, version).unwrap();\n");
            content.push_str("        });\n");
        }
    } else if field.is_array == Some(true) {
        let item_type = field.item_type.as_deref().unwrap_or("i32");
        if is_primitive_type(item_type) {
            content.push_str(&format!("        encode_array(buf, &self.{}, |b, item| {{\n", field_name));
            content.push_str(&format!("            {}\n", get_encode_primitive(item_type, "item")));
            content.push_str("        });\n");
        } else {
            content.push_str(&format!("        encode_array(buf, &self.{}, |b, item| {{\n", field_name));
            content.push_str("            item.encode(b, version).unwrap();\n");
            content.push_str("        });\n");
        }
    } else if field.is_map == Some(true) {
        content.push_str(&format!("        // TODO: Encode map {}\n", field_name));
    } else if is_primitive_type(ty) {
        content.push_str(&format!("        {};\n", get_encode_primitive(ty, &format!("self.{}", field_name))));
    } else if ty == "string" {
        if field.nullable == Some(true) {
            content.push_str(&format!("        encode_nullable_string(buf, &self.{});\n", field_name));
        } else {
            content.push_str(&format!("        encode_string(buf, &self.{});\n", field_name));
        }
    } else if ty == "bytes" {
        if field.nullable == Some(true) {
            content.push_str(&format!("        encode_compact_nullable_bytes(buf, &self.{}.as_ref().map(|v| v.clone()));\n", field_name));
        } else {
            content.push_str(&format!("        encode_compact_bytes(buf, &self.{});\n", field_name));
        }
    } else if ty == "uuid" {
        content.push_str(&format!("        encode_uuid(buf, &self.{});\n", field_name));
    } else if ty == "records" {
        content.push_str(&format!("        encode_records(buf, &self.{});\n", field_name));
    } else {
        content.push_str(&format!("        self.{}.encode(buf, version)?;\n", field_name));
    }

    content
}

fn generate_decode_body(fields: &[FieldDef]) -> String {
    let mut content = String::new();

    content.push_str("        let mut result = Self::default();\n\n");

    for field in fields {
        let field_name = to_snake_case(&field.name);

        if let Some(ref versions) = field.versions {
            let (min_ver, max_ver) = parse_version_range(versions);
            if let Some(min) = min_ver {
                content.push_str(&format!("        if version >= {} {{\n", min));
                if let Some(max) = max_ver {
                    content.push_str(&format!("            if version <= {} {{\n", max));
                    content.push_str(&generate_field_decode(field, &field_name));
                    content.push_str("            }\n");
                } else {
                    content.push_str(&generate_field_decode(field, &field_name));
                }
                content.push_str("        }\n");
            }
        } else {
            content.push_str(&generate_field_decode(field, &field_name));
        }
    }

    content.push('\n');
    content.push_str("        Ok(result)\n");

    content
}

fn generate_field_decode(field: &FieldDef, field_name: &str) -> String {
    let mut content = String::new();

    if field.tagged == Some(true) && field.tag.is_some() {
        let tag = field.tag.unwrap();
        content.push_str(&format!("        // Tagged field: {} (tag={})\n", field_name, tag));
        return content;
    }

    // 检查类型是否以 [] 开头表示数组
    let ty = &field.ty;
    if ty.starts_with("[]") {
        let item_type = &ty[2..]; // 去掉 []
        if is_primitive_type(item_type) {
            content.push_str(&format!("        result.{} = decode_array(buf, |b| {{\n", field_name));
            content.push_str(&format!("            Ok({})\n", get_decode_primitive(item_type)));
            content.push_str("        })?;\n");
        } else {
            content.push_str(&format!("        result.{} = decode_array(buf, |b| {{\n", field_name));
            content.push_str(&format!("            {}::decode(b, version)\n", to_camel_case(item_type)));
            content.push_str("        })?;\n");
        }
    } else if field.is_array == Some(true) {
        let item_type = field.item_type.as_deref().unwrap_or("i32");
        if is_primitive_type(item_type) {
            content.push_str(&format!("        result.{} = decode_array(buf, |b| {{\n", field_name));
            content.push_str(&format!("            Ok({})\n", get_decode_primitive(item_type)));
            content.push_str("        })?;\n");
        } else {
            content.push_str(&format!("        result.{} = decode_array(buf, |b| {{\n", field_name));
            content.push_str(&format!("            {}::decode(b, version)\n", to_camel_case(item_type)));
            content.push_str("        })?;\n");
        }
    } else if field.is_map == Some(true) {
        content.push_str(&format!("        // TODO: Decode map {}\n", field_name));
    } else if is_primitive_type(ty) {
        content.push_str(&format!("        result.{} = {};\n", field_name, get_decode_primitive(ty)));
    } else if ty == "string" {
        if field.nullable == Some(true) {
            content.push_str(&format!("        result.{} = decode_nullable_string(buf)?;\n", field_name));
        } else {
            content.push_str(&format!("        result.{} = decode_string(buf)?;\n", field_name));
        }
    } else if ty == "bytes" {
        if field.nullable == Some(true) {
            content.push_str(&format!("        result.{} = decode_compact_nullable_bytes(buf)?.map(|b| b.to_vec());\n", field_name));
        } else {
            content.push_str(&format!("        result.{} = decode_compact_bytes(buf)?.to_vec();\n", field_name));
        }
    } else if ty == "uuid" {
        content.push_str(&format!("        result.{} = decode_uuid(buf)?;\n", field_name));
    } else if ty == "records" {
        content.push_str(&format!("        result.{} = decode_records(buf)?;\n", field_name));
    } else {
        content.push_str(&format!("        result.{} = {}::decode(buf, version)?;\n", field_name, to_camel_case(ty)));
    }

    content
}

fn generate_mod_rs(modules: &[(String, i16)], output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = output_dir.join("mod.rs");
    let mut file = File::create(&file_path)?;

    let mut content = String::new();
    content.push_str("//! Kafka API 协议定义\n");
    content.push_str("//!\n");
    content.push_str("//! 本模块由代码生成器自动生成，请勿手动修改\n\n");

    for (module_name, _) in modules {
        content.push_str(&format!("pub mod {};\n", module_name));
    }

    content.push('\n');

    for (module_name, _) in modules {
        let api_name = to_camel_case(module_name);
        content.push_str(&format!("pub use {}::{}Request;\n", module_name, api_name));
        content.push_str(&format!("pub use {}::{}Response;\n", module_name, api_name));
    }

    file.write_all(content.as_bytes())?;
    Ok(())
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
        "int16" | "i16" => "i16".to_string(),
        "int32" | "i32" | "uint32" => "i32".to_string(),
        "int64" | "i64" | "uint64" => "i64".to_string(),
        "string" => "String".to_string(),
        "bytes" => "Vec<u8>".to_string(),
        "records" => "Vec<u8>".to_string(),
        "uuid" => "Uuid".to_string(),
        other => to_camel_case(other),
    }
}

fn is_primitive_type(ty: &str) -> bool {
    matches!(
        ty,
        "bool" | "int8" | "i8" | "int16" | "i16" | "int32" | "i32" | "uint32" | "int64" | "i64"
            | "uint64"
    )
}

fn get_encode_primitive(ty: &str, value: &str) -> String {
    match ty {
        "bool" => format!("buf.put_u8(if {} {{ 1 }} else {{ 0 }})", value),
        "int8" | "i8" => format!("buf.put_i8({})", value),
        "int16" | "i16" => format!("buf.put_i16({})", value),
        "int32" | "i32" | "uint32" => format!("buf.put_i32({})", value),
        "int64" | "i64" | "uint64" => format!("buf.put_i64({})", value),
        _ => format!("// TODO: encode {}", ty),
    }
}

fn get_decode_primitive(ty: &str) -> String {
    match ty {
        "bool" => "buf.get_u8() != 0".to_string(),
        "int8" | "i8" => "buf.get_i8()".to_string(),
        "int16" | "i16" => "buf.get_i16()".to_string(),
        "int32" | "i32" | "uint32" => "buf.get_i32()".to_string(),
        "int64" | "i64" | "uint64" => "buf.get_i64()".to_string(),
        _ => format!("// TODO: decode {}", ty),
    }
}

fn parse_version_range(versions: &str) -> (Option<i16>, Option<i16>) {
    if versions.ends_with('+') {
        let min = versions.trim_end_matches('+').parse().ok();
        (min, None)
    } else if versions.contains('-') {
        let parts: Vec<_> = versions.split('-').collect();
        let min = parts[0].parse().ok();
        let max = parts.get(1).and_then(|s| s.parse().ok());
        (min, max)
    } else {
        (versions.parse().ok(), None)
    }
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_lowercase().next().unwrap());
    }
    result
}

fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize = true;
    for c in s.chars() {
        if c == '_' {
            capitalize = true;
        } else if capitalize {
            result.push(c.to_uppercase().next().unwrap());
            capitalize = false;
        } else {
            result.push(c);
        }
    }
    result
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

#[derive(Debug, Clone, serde::Deserialize)]
struct ApiDefinition {
    #[serde(rename = "apiKey")]
    api_key: i16,
    #[serde(rename = "type")]
    #[serde(default)]
    ty: String,
    #[serde(default)]
    name: String,
    #[serde(rename = "validVersions")]
    #[serde(default)]
    valid_versions: String,
    #[serde(rename = "flexibleVersions")]
    #[serde(default)]
    flexible_versions: String,
    #[serde(default)]
    fields: Option<Vec<FieldDef>>,
}

impl ApiDefinition {
    fn min_version(&self) -> i16 {
        parse_version_range(&self.valid_versions).0.unwrap_or(0)
    }

    fn max_version(&self) -> i16 {
        parse_version_range(&self.valid_versions).1.unwrap_or(i16::MAX)
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct FieldDef {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    versions: Option<String>,
    #[serde(default)]
    nullable: Option<bool>,
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    doc: Option<String>,
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
    #[serde(default)]
    flexible: Option<bool>,
    #[serde(default)]
    tagged: Option<bool>,
    #[serde(default)]
    tag: Option<i32>,
    #[serde(default)]
    fields: Option<Vec<FieldDef>>,
}
