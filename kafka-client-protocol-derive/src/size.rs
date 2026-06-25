// src/size.rs
use crate::field::FieldInfo;
use crate::version_range::VersionRange;
use proc_macro2::TokenStream;
use quote::quote;

/// 生成 size 和 flexible_size 方法
pub fn generate_size(fields: &[FieldInfo], flexible_version: Option<i16>) -> TokenStream {
    let size_method = generate_size_single(fields, flexible_version, false);
    let flexible_size_method = generate_size_single(fields, flexible_version, true);

    quote! {
        #size_method
        #flexible_size_method
    }
}

/// 生成单个计算大小方法（size 或 flexible_size）
fn generate_size_single(
    fields: &[FieldInfo],
    _flexible_version: Option<i16>,
    flexible: bool,
) -> TokenStream {
    let method_name = if flexible {
        syn::Ident::new("flexible_size", proc_macro2::Span::call_site())
    } else {
        syn::Ident::new("size", proc_macro2::Span::call_site())
    };

    let size_fields: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_size_field(field, flexible))
        .collect();

    // 灵活版本：空的 tagged fields 占 1 byte (varint 0)
    let tagged_overhead = if flexible {
        quote! { total += 1; }
    } else {
        TokenStream::new()
    };

    quote! {
        fn #method_name(&self, version: i16) -> usize {
            use kafka_client_protocol_core::codec::*;

            let mut total = 0;
            #(#size_fields)*

            #tagged_overhead

            total
        }
    }
}

/// 生成单个字段的大小计算
fn generate_size_field(field: &FieldInfo, flexible: bool) -> TokenStream {
    let field_name = &field.name;
    let condition = field.versions.as_check_expr();

    // 标签字段
    if field.tagged_versions.is_some() {
        if flexible {
            // Tagged 字段仅在 flexible 模式下编码
            return quote! {
                if #condition {
                    if !self.#field_name.is_default() {
                        total += varint_len(self.#field_name.flexible_size(version) as u32 + 1);
                        total += self.#field_name.flexible_size(version);
                    }
                }
            };
        } else {
            // 非 flexible 模式下 tagged 字段不存在
            return TokenStream::new();
        }
    }

    // 可空字段 - 只有当类型是 Option 时才生成 is_none 检查
    if let Some(nullable_versions) = &field.nullable_versions {
        let null_condition = nullable_versions.as_check_expr();

        if field.is_option() {
            // Option 类型：检查 is_none()
            if flexible {
                return quote! {
                    if #condition {
                        if #null_condition && self.#field_name.is_none() {
                            total += 1; // varint(0)
                        } else {
                            total += self.#field_name.flexible_size(version);
                        }
                    }
                };
            } else {
                return quote! {
                    if #condition {
                        if #null_condition && self.#field_name.is_none() {
                            total += 4; // -1
                        } else {
                            total += self.#field_name.size(version);
                        }
                    }
                };
            }
        } else {
            // 非 Option 类型（如 int 使用 default 值表示空）
            return quote! {
                if #condition {
                    total += self.#field_name.size(version);
                }
            };
        }
    }

    // 普通字段
    let size_call = if flexible {
        quote! { total += self.#field_name.flexible_size(version); }
    } else {
        quote! { total += self.#field_name.size(version); }
    };

    if field.versions == VersionRange::All {
        size_call
    } else {
        quote! {
            if #condition {
                #size_call
            }
        }
    }
}
