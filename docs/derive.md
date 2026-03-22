## 模仿 Prost 的 Derive 方式实现 KafkaMessage

Prost 的派生宏是 Rust 过程宏设计的典范。让我们深入分析其实现方式，并应用到 Kafka 协议中。

### 一、Prost 的 Derive 机制分析

#### 1.1 Prost 的使用方式

```rust
// 用户代码
use prost::Message;

#[derive(Message)]
pub struct Person {
    #[prost(string, tag = "1")]
    pub name: String,
    
    #[prost(int32, tag = "2")]
    pub age: i32,
    
    #[prost(message, tag = "3")]
    pub address: Option<Address>,
}
```

#### 1.2 Prost 的派生宏结构

```rust
// prost-derive/src/lib.rs 的核心结构
#[proc_macro_derive(Message, attributes(prost))]
pub fn message_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    // 1. 解析结构体信息
    let struct_name = input.ident;
    let fields = parse_fields(&input);
    
    // 2. 生成 encode 方法
    let encode_impl = generate_encode(&fields);
    
    // 3. 生成 decode 方法
    let decode_impl = generate_decode(&fields);
    
    // 4. 生成 encoded_len 方法
    let len_impl = generate_len(&fields);
    
    // 5. 生成 Message trait 实现
    quote! {
        impl ::prost::Message for #struct_name {
            #encode_impl
            #decode_impl
            #len_impl
        }
    }.into()
}
```

### 二、KafkaMessage 派生宏的完整实现

#### 2.1 项目结构

```
kafka-protocol-derive/
├── Cargo.toml
└── src/
    ├── lib.rs              # 派生宏入口
    ├── ast.rs              # AST 解析
    ├── encode.rs           # encode 代码生成
    ├── decode.rs           # decode 代码生成
    ├── version.rs          # 版本范围解析
    └── field.rs            # 字段信息结构
```

#### 2.2 Cargo.toml

```toml
[package]
name = "kafka-protocol-derive"
version = "0.1.0"
edition = "2021"

[lib]
proc-macro = true

[dependencies]
syn = { version = "2.0", features = ["full", "extra-traits"] }
quote = "1.0"
proc-macro2 = "1.0"

[dev-dependencies]
kafka-protocol = { path = "../kafka-protocol" }
```

#### 2.3 核心 AST 解析 (ast.rs)

```rust
// kafka-protocol-derive/src/ast.rs
use syn::{DeriveInput, Field, Meta, NestedMeta, Lit, Ident, Type};
use proc_macro2::Span;
use std::collections::HashMap;

/// 解析后的消息信息
pub struct MessageInfo {
    pub name: Ident,
    pub api_key: Option<i16>,
    pub valid_versions: VersionRange,
    pub flexible_versions: Option<VersionRange>,
    pub fields: Vec<FieldInfo>,
}

/// 解析后的字段信息
pub struct FieldInfo {
    pub name: Ident,
    pub ty: Type,
    pub versions: VersionRange,
    pub nullable: bool,
    pub flexible: bool,
    pub default: Option<String>,
    pub tag: Option<u32>,
    pub about: Option<String>,
}

/// 版本范围
#[derive(Debug, Clone)]
pub enum VersionRange {
    Exact(i16),
    Range(i16, i16),  // inclusive
    From(i16),        // 版本+ 格式
    All,
}

impl VersionRange {
    pub fn parse(s: &str) -> Self {
        let s = s.trim();
        
        if s == "0+" || s == "none" || s == "all" {
            return VersionRange::All;
        }
        
        if s.contains('-') {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() == 2 {
                let start = parts[0].parse::<i16>().unwrap();
                let end = parts[1].parse::<i16>().unwrap();
                return VersionRange::Range(start, end);
            }
        }
        
        if s.ends_with('+') {
            let start = s.trim_end_matches('+').parse::<i16>().unwrap();
            return VersionRange::From(start);
        }
        
        VersionRange::Exact(s.parse().unwrap())
    }
    
    pub fn contains(&self, version: i16) -> bool {
        match self {
            VersionRange::Exact(v) => version == *v,
            VersionRange::Range(start, end) => version >= *start && version <= *end,
            VersionRange::From(start) => version >= *start,
            VersionRange::All => true,
        }
    }
    
    pub fn min_version(&self) -> i16 {
        match self {
            VersionRange::Exact(v) => *v,
            VersionRange::Range(start, _) => *start,
            VersionRange::From(start) => *start,
            VersionRange::All => 0,
        }
    }
    
    pub fn max_version(&self) -> Option<i16> {
        match self {
            VersionRange::Exact(v) => Some(*v),
            VersionRange::Range(_, end) => Some(*end),
            VersionRange::From(_) => None,
            VersionRange::All => None,
        }
    }
    
    pub fn as_check_expr(&self) -> proc_macro2::TokenStream {
        match self {
            VersionRange::Exact(v) => quote::quote! { version == #v },
            VersionRange::Range(start, end) => quote::quote! { version >= #start && version <= #end },
            VersionRange::From(start) => quote::quote! { version >= #start },
            VersionRange::All => quote::quote! { true },
        }
    }
}

/// 解析结构体
pub fn parse_message(input: &DeriveInput) -> syn::Result<MessageInfo> {
    let name = input.ident.clone();
    
    // 解析结构体属性
    let mut api_key = None;
    let mut valid_versions = VersionRange::All;
    let mut flexible_versions = None;
    
    for attr in &input.attrs {
        if attr.path().is_ident("kafka") {
            parse_message_attrs(attr, &mut api_key, &mut valid_versions, &mut flexible_versions)?;
        }
    }
    
    // 解析字段
    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => {
                let mut field_infos = Vec::new();
                for field in &fields.named {
                    if let Some(info) = parse_field(field)? {
                        field_infos.push(info);
                    }
                }
                field_infos
            }
            _ => return Err(syn::Error::new_spanned(input, "Only named fields are supported")),
        },
        _ => return Err(syn::Error::new_spanned(input, "Only structs are supported")),
    };
    
    Ok(MessageInfo {
        name,
        api_key,
        valid_versions,
        flexible_versions,
        fields,
    })
}

/// 解析消息属性
fn parse_message_attrs(
    attr: &syn::Attribute,
    api_key: &mut Option<i16>,
    valid_versions: &mut VersionRange,
    flexible_versions: &mut Option<VersionRange>,
) -> syn::Result<()> {
    let meta = attr.parse_meta()?;
    
    if let syn::Meta::List(list) = meta {
        for nested in list.nested {
            if let NestedMeta::Meta(meta) = nested {
                match meta.path().get_ident().map(|i| i.to_string()).as_deref() {
                    Some("api_key") => {
                        if let syn::Meta::NameValue(nv) = meta {
                            if let syn::Lit::Int(lit) = nv.lit {
                                *api_key = Some(lit.base10_parse()?);
                            }
                        }
                    }
                    Some("valid_versions") => {
                        if let syn::Meta::NameValue(nv) = meta {
                            if let syn::Lit::Str(lit) = nv.lit {
                                *valid_versions = VersionRange::parse(&lit.value());
                            }
                        }
                    }
                    Some("flexible_versions") => {
                        if let syn::Meta::NameValue(nv) = meta {
                            if let syn::Lit::Str(lit) = nv.lit {
                                *flexible_versions = Some(VersionRange::parse(&lit.value()));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    
    Ok(())
}

/// 解析字段属性
fn parse_field(field: &Field) -> syn::Result<Option<FieldInfo>> {
    let name = field.ident.clone().unwrap();
    let ty = field.ty.clone();
    
    let mut versions = VersionRange::All;
    let mut nullable = false;
    let mut flexible = false;
    let mut default = None;
    let mut tag = None;
    let mut about = None;
    
    for attr in &field.attrs {
        if attr.path().is_ident("kafka") {
            let meta = attr.parse_meta()?;
            
            if let syn::Meta::List(list) = meta {
                for nested in list.nested {
                    if let NestedMeta::Meta(meta) = nested {
                        match meta.path().get_ident().map(|i| i.to_string()).as_deref() {
                            Some("versions") => {
                                if let syn::Meta::NameValue(nv) = meta {
                                    if let syn::Lit::Str(lit) = nv.lit {
                                        versions = VersionRange::parse(&lit.value());
                                    }
                                }
                            }
                            Some("nullable") => {
                                nullable = true;
                            }
                            Some("flexible") => {
                                flexible = true;
                            }
                            Some("default") => {
                                if let syn::Meta::NameValue(nv) = meta {
                                    if let syn::Lit::Str(lit) = nv.lit {
                                        default = Some(lit.value());
                                    }
                                }
                            }
                            Some("tag") => {
                                if let syn::Meta::NameValue(nv) = meta {
                                    if let syn::Lit::Int(lit) = nv.lit {
                                        tag = Some(lit.base10_parse()?);
                                    }
                                }
                            }
                            Some("about") => {
                                if let syn::Meta::NameValue(nv) = meta {
                                    if let syn::Lit::Str(lit) = nv.lit {
                                        about = Some(lit.value());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
    
    Ok(Some(FieldInfo {
        name,
        ty,
        versions,
        nullable,
        flexible,
        default,
        tag,
        about,
    }))
}
```

#### 2.4 encode 代码生成 (encode.rs)

```rust
// kafka-protocol-derive/src/encode.rs
use quote::quote;
use proc_macro2::TokenStream;
use crate::ast::{FieldInfo, VersionRange};

/// 生成 encode 方法
pub fn generate_encode(fields: &[FieldInfo]) -> TokenStream {
    let encode_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_encode_field(field))
        .collect();
    
    quote! {
        fn encode(&self, buf: &mut ::bytes::BytesMut, version: i16) -> ::kafka_protocol::ProtocolResult<()> {
            use ::kafka_protocol::codec::*;
            
            #(#encode_fields)*
            
            Ok(())
        }
    }
}

/// 生成单个字段的编码代码
fn generate_encode_field(field: &FieldInfo) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let is_nullable = field.nullable;
    let is_flexible = field.flexible;
    
    // 根据字段类型生成编码代码
    let encode_body = generate_encode_body(field, is_flexible);
    
    if field.versions != VersionRange::All {
        quote! {
            if #condition {
                #encode_body
            }
        }
    } else {
        encode_body
    }
}

/// 根据类型生成具体的编码代码
fn generate_encode_body(field: &FieldInfo, is_flexible: bool) -> TokenStream {
    let field_name = &field.name;
    let ty = &field.ty;
    
    // 类型匹配
    if let syn::Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();
            
            match type_name.as_str() {
                "String" => {
                    return generate_encode_string(field_name, is_flexible);
                }
                "Option" => {
                    return generate_encode_option(field_name, is_flexible);
                }
                "Vec" => {
                    return generate_encode_vec(field_name, is_flexible);
                }
                "bool" => {
                    return quote! {
                        buf.put_i8(self.#field_name as i8);
                    };
                }
                "i8" | "i16" | "i32" | "i64" => {
                    let put_method = format!("put_{}", type_name);
                    let put_ident = syn::Ident::new(&put_method, proc_macro2::Span::call_site());
                    return quote! {
                        buf.#put_ident(self.#field_name);
                    };
                }
                _ => {}
            }
        }
    }
    
    // 默认：使用 Message trait 的 encode
    quote! {
        self.#field_name.encode(buf, version)?;
    }
}

/// 编码字符串
fn generate_encode_string(field_name: &syn::Ident, is_flexible: bool) -> TokenStream {
    if is_flexible {
        quote! {
            encode_compact_string(buf, &self.#field_name);
        }
    } else {
        quote! {
            encode_string(buf, &self.#field_name);
        }
    }
}

/// 编码 Option 类型
fn generate_encode_option(field_name: &syn::Ident, is_flexible: bool) -> TokenStream {
    if is_flexible {
        quote! {
            encode_compact_nullable_string(buf, &self.#field_name);
        }
    } else {
        quote! {
            encode_nullable_string(buf, &self.#field_name);
        }
    }
}

/// 编码 Vec 类型
fn generate_encode_vec(field_name: &syn::Ident, is_flexible: bool) -> TokenStream {
    if is_flexible {
        quote! {
            encode_compact_array(buf, &self.#field_name, |buf, item| {
                item.encode(buf, version)?;
                Ok(())
            });
        }
    } else {
        quote! {
            encode_array(buf, &self.#field_name, |buf, item| {
                item.encode(buf, version)?;
                Ok(())
            });
        }
    }
}
```

#### 2.5 decode 代码生成 (decode.rs)

```rust
// kafka-protocol-derive/src/decode.rs
use quote::quote;
use proc_macro2::TokenStream;
use crate::ast::{FieldInfo, VersionRange};

/// 生成 decode 方法
pub fn generate_decode(fields: &[FieldInfo]) -> TokenStream {
    let decode_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_decode_field(field))
        .collect();
    
    quote! {
        fn decode(buf: &mut ::bytes::Bytes, version: i16) -> ::kafka_protocol::ProtocolResult<Self> {
            use ::kafka_protocol::codec::*;
            
            let mut msg = Self::default();
            
            #(#decode_fields)*
            
            Ok(msg)
        }
    }
}

/// 生成单个字段的解码代码
fn generate_decode_field(field: &FieldInfo) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();
    let is_flexible = field.flexible;
    let default = &field.default;
    
    let decode_body = generate_decode_body(field, is_flexible);
    
    let field_assignment = if field.nullable {
        quote! {
            msg.#field_name = #decode_body;
        }
    } else {
        quote! {
            msg.#field_name = #decode_body?;
        }
    };
    
    if field.versions != VersionRange::All {
        quote! {
            if #condition {
                #field_assignment
            } else if let Some(default) = #default {
                msg.#field_name = default;
            }
        }
    } else {
        field_assignment
    }
}

/// 根据类型生成具体的解码代码
fn generate_decode_body(field: &FieldInfo, is_flexible: bool) -> TokenStream {
    let field_name = &field.name;
    let ty = &field.ty;
    
    // 类型匹配
    if let syn::Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let type_name = seg.ident.to_string();
            
            match type_name.as_str() {
                "String" => {
                    return generate_decode_string(is_flexible);
                }
                "Option" => {
                    return generate_decode_option(is_flexible);
                }
                "Vec" => {
                    return generate_decode_vec(is_flexible);
                }
                "bool" => {
                    return quote! { buf.get_i8() != 0 };
                }
                "i8" | "i16" | "i32" | "i64" => {
                    let get_method = format!("get_{}", type_name);
                    let get_ident = syn::Ident::new(&get_method, proc_macro2::Span::call_site());
                    return quote! { buf.#get_ident() };
                }
                _ => {}
            }
        }
    }
    
    // 默认：使用 Message trait 的 decode
    quote! {
        <#ty as ::kafka_protocol::Message>::decode(buf, version)?
    }
}

fn generate_decode_string(is_flexible: bool) -> TokenStream {
    if is_flexible {
        quote! { decode_compact_string(buf)? }
    } else {
        quote! { decode_string(buf)? }
    }
}

fn generate_decode_option(is_flexible: bool) -> TokenStream {
    if is_flexible {
        quote! { decode_compact_nullable_string(buf)? }
    } else {
        quote! { decode_nullable_string(buf)? }
    }
}

fn generate_decode_vec(is_flexible: bool) -> TokenStream {
    if is_flexible {
        quote! { decode_compact_array(buf, |b| <_>::decode(b, version))? }
    } else {
        quote! { decode_array(buf, |b| <_>::decode(b, version))? }
    }
}
```

#### 2.6 主入口 (lib.rs)

```rust
// kafka-protocol-derive/src/lib.rs
mod ast;
mod encode;
mod decode;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// 派生 KafkaMessage trait
///
/// # Example
/// ```rust
/// use kafka_protocol::{KafkaMessage, Message};
///
/// #[derive(KafkaMessage, Debug, Clone, Default)]
/// #[kafka(api_key = 3, valid_versions = "0-12")]
/// pub struct MetadataRequest {
///     #[kafka(versions = "0+", nullable)]
///     pub topics: Option<Vec<String>>,
///     
///     #[kafka(versions = "4+")]
///     pub allow_auto_topic_creation: bool,
/// }
/// ```
#[proc_macro_derive(KafkaMessage, attributes(kafka))]
pub fn kafka_message_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    
    // 解析结构体信息
    let info = match ast::parse_message(&input) {
        Ok(info) => info,
        Err(e) => return e.to_compile_error().into(),
    };
    
    let struct_name = info.name;
    let api_key = info.api_key;
    let valid_versions = info.valid_versions;
    let fields = &info.fields;
    
    // 生成 encode/decode 方法
    let encode_impl = encode::generate_encode(fields);
    let decode_impl = decode::generate_decode(fields);
    
    // 生成 encoded_len 方法（可选）
    let len_impl = generate_encoded_len(fields);
    
    // 生成 api_key 方法
    let api_key_impl = if let Some(key) = api_key {
        quote! { Some(#key) }
    } else {
        quote! { None }
    };
    
    let expanded = quote! {
        impl ::kafka_protocol::Message for #struct_name {
            fn type_name() -> &'static str {
                stringify!(#struct_name)
            }
            
            fn api_key(&self) -> Option<i16> {
                #api_key_impl
            }
            
            fn default_version() -> i16 {
                #valid_versions.min_version()
            }
            
            #encode_impl
            #decode_impl
            #len_impl
        }
    };
    
    TokenStream::from(expanded)
}

/// 生成 encoded_len 方法
fn generate_encoded_len(fields: &[ast::FieldInfo]) -> proc_macro2::TokenStream {
    // 可以计算精确长度，简化版本直接调用 encode
    quote! {
        fn encoded_len(&self, version: i16) -> usize {
            let mut buf = ::bytes::BytesMut::new();
            self.encode(&mut buf, version).unwrap();
            buf.len()
        }
    }
}
```

### 三、使用生成的派生宏

```rust
// 用户代码
use kafka_protocol::{KafkaMessage, Message};

#[derive(KafkaMessage, Debug, Clone, Default)]
#[kafka(api_key = 3, valid_versions = "0-12", flexible_versions = "9+")]
pub struct MetadataRequest {
    /// The topics to fetch metadata for
    #[kafka(versions = "0+", nullable)]
    pub topics: Option<Vec<String>>,
    
    /// If true, auto-create topics
    #[kafka(versions = "4+")]
    pub allow_auto_topic_creation: bool,
}

#[derive(KafkaMessage, Debug, Clone, Default)]
#[kafka(api_key = 3)]
pub struct MetadataResponse {
    #[kafka(versions = "0+")]
    pub brokers: Vec<Broker>,
    
    #[kafka(versions = "2+", nullable)]
    pub cluster_id: Option<String>,
    
    #[kafka(versions = "1+")]
    pub controller_id: Option<i32>,
    
    #[kafka(versions = "0+")]
    pub topics: Vec<Topic>,
}

// 使用
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
    };
    
    let version = 12;
    let mut buf = bytes::BytesMut::new();
    request.encode(&mut buf, version)?;
    
    println!("Encoded {} bytes", buf.len());
    Ok(())
}
```

### 四、与 build.rs 集成

```rust
// build.rs
use std::fs;
use std::path::Path;
use quote::quote;

fn main() {
    let json_files = get_kafka_json_files();
    let mut struct_defs = Vec::new();
    
    for json_file in json_files {
        let spec = parse_json(&json_file);
        let struct_def = generate_struct_definition(&spec);
        struct_defs.push(struct_def);
    }
    
    let expanded = quote! {
        use kafka_protocol::KafkaMessage;
        
        #(#struct_defs)*
    };
    
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generated.rs");
    fs::write(dest_path, expanded.to_string()).unwrap();
}

/// 从 JSON 生成带 #[derive(KafkaMessage)] 的结构体
fn generate_struct_definition(spec: &Value) -> proc_macro2::TokenStream {
    let name = syn::Ident::new(spec["name"].as_str().unwrap(), proc_macro2::Span::call_site());
    let api_key = spec["apiKey"].as_u64().unwrap();
    let valid_versions = spec["validVersions"].as_str().unwrap();
    let flexible_versions = spec.get("flexibleVersions")
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    
    let fields = spec["fields"].as_array().unwrap();
    let mut field_defs = Vec::new();
    
    for field in fields {
        let field_name = syn::Ident::new(&to_snake_case(field["name"].as_str().unwrap()), proc_macro2::Span::call_site());
        let rust_type = map_kafka_type_to_rust(field);
        let versions = field["versions"].as_str().unwrap();
        let nullable = field.get("type").and_then(|t| t.as_str()).unwrap_or("").contains("nullable");
        let about = field.get("about").and_then(|a| a.as_str()).unwrap_or("");
        
        let field_def = quote! {
            #[doc = #about]
            #[kafka(versions = #versions, #(nullable)?)]
            pub #field_name: #rust_type
        };
        field_defs.push(field_def);
    }
    
    quote! {
        #[derive(KafkaMessage, Debug, Clone, Default)]
        #[kafka(api_key = #api_key, valid_versions = #valid_versions, flexible_versions = #flexible_versions)]
        pub struct #name {
            #(#field_defs)*
        }
    }
}
```

### 五、与 Prost 的对比

| 特性 | Prost 实现 | KafkaMessage 实现 |
|------|-----------|-------------------|
| 派生宏 | `#[derive(Message)]` | `#[derive(KafkaMessage)]` |
| 字段属性 | `#[prost(type, tag = "n")]` | `#[kafka(versions = "0+", nullable)]` |
| 结构体属性 | 无 | `#[kafka(api_key = 3, valid_versions = "0-12")]` |
| 版本处理 | 无（Protobuf 无版本概念） | 内置版本范围支持 |
| 编码方式 | 基于 tag 的 protobuf 格式 | Kafka 二进制协议 |
| 柔性格式 | 无 | 支持 flexible format (v9+) |

### 六、总结

模仿 Prost 的 derive 方式实现 KafkaMessage 的核心步骤：

1. **定义属性语法**：`#[kafka(api_key = 3, valid_versions = "0-12")]` 用于结构体，`#[kafka(versions = "4+", nullable)]` 用于字段

2. **AST 解析**：使用 `syn` 解析结构体、字段和属性，提取版本范围、类型信息

3. **代码生成**：根据字段类型和属性生成 encode/decode 代码，处理版本条件

4. **与 build.rs 集成**：从 JSON 定义生成带 `#[derive(KafkaMessage)]` 的结构体

这种设计让用户代码非常简洁，同时保持了 Kafka 协议所需的版本感知能力，完美平衡了易用性和灵活性。