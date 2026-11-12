use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::Parser;

pub(crate) fn ducklake_table_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    let table_name = match parse_table_name(args.into()) {
        Ok(table_name) => table_name,
        Err(error) => return error.into_compile_error().into(),
    };
    let mut ast = syn::parse_macro_input!(input as syn::DeriveInput);
    let ducklake_table = ducklake_table_blocks(&mut ast, table_name);
    let result = quote! {
        #[derive(sqlx::FromRow, Debug, Clone)]
        #ast
        #ducklake_table
    };
    result.into()
}

fn parse_table_name(args: proc_macro2::TokenStream) -> syn::Result<Option<String>> {
    let mut table_name = None;
    syn::meta::parser(|meta| {
        if !meta.path.is_ident("table_name") {
            return Err(meta.error("expected `table_name = \"...\"`"));
        }
        if table_name.is_some() {
            return Err(meta.error("duplicate `table_name`"));
        }
        table_name = Some(meta.value()?.parse::<syn::LitStr>()?.value());
        Ok(())
    })
    .parse2(args)?;
    Ok(table_name)
}

fn ducklake_table_blocks(
    ast: &mut syn::DeriveInput,
    table_name: Option<String>,
) -> proc_macro2::TokenStream {
    let visibility = &ast.vis;
    let camel_name = &ast.ident;
    let snake_name_str = camel_name.to_string().to_snake_case();
    let snake_name = format_ident!("{}", snake_name_str);
    let table_name = table_name.unwrap_or(snake_name_str);

    let fields: Vec<_> = match &mut ast.data {
        syn::Data::Struct(s) => match &mut s.fields {
            syn::Fields::Named(nf) => nf.named.iter_mut().map(Field::parse).collect(),
            _ => panic!("#[ducklake_table] only allows named struct fields."),
        },
        _ => panic!("#[ducklake_table] can only be used on structs."),
    };

    let enum_name = quote! { #snake_name::Column };
    let column_defs = fields.iter().map(|f| f.to_column_def(&enum_name));

    let column_snake_names: Vec<_> = fields.iter().map(|f| f.snake_name).collect();
    let column_camel_names: Vec<_> = fields.iter().map(|f| &f.camel_name).collect();
    let column_names: Vec<_> = column_snake_names
        .iter()
        .map(|name| name.to_string().trim_start_matches("r#").to_owned())
        .collect();

    quote! {
        #visibility mod #snake_name {
            #[derive(sea_query::Iden)]
            #[iden = #table_name]
            #visibility struct Table;

            #[derive(sea_query::Iden, strum::EnumIter, Clone, Copy, PartialEq, Eq)]
            #visibility enum Column {
                #(#column_camel_names,)*
            }

            impl Column {
                #visibility fn col(self) -> sea_query::Expr {
                    sea_query::Expr::col(self)
                }
            }
        }

        impl sea_query_ext::CreatableEntity for #camel_name {
            fn create_table(dialect: crate::db::Dialect) -> sea_query::TableCreateStatement {
                sea_query::Table::create()
                    .table(#snake_name::Table)
                    #(.col(#column_defs))*
                    .to_owned()
            }
        }

        impl sea_query_ext::InsertableEntity for #camel_name {
            const TABLE: &'static str = #table_name;
            const COLUMNS: &'static [&'static str] = &[#(#column_names,)*];

            fn into_values(self) -> Vec<sea_query::Value> {
                vec![#(self.#column_snake_names.into(),)*]
            }
        }
    }
}

struct Field<'a> {
    snake_name: &'a proc_macro2::Ident,
    camel_name: proc_macro2::Ident,
    is_nullable: bool,
    is_primary_key: bool,
    column_type: proc_macro2::TokenStream,
}

impl<'a> Field<'a> {
    fn parse(field: &'a mut syn::Field) -> Self {
        let ident = field.ident.as_ref().unwrap();

        let str_ident = ident.to_string();
        let camel_name = format_ident!(
            "{}",
            str_ident
                .strip_prefix("r#")
                .unwrap_or(&str_ident)
                .to_upper_camel_case()
        );

        let field_type = &field.ty;
        let ty_str = quote! { #field_type }.to_string().replace(' ', "");
        let ty_str = if ty_str.starts_with("Option<") {
            &ty_str[7..(ty_str.len() - 1)]
        } else {
            &ty_str
        };
        // NOTE: Strings are unbounded by default which some databases (e.g. MySQL) do not permit
        //  for columns that are part of a key. Such columns must set `#[max_length = ...]`.
        let max_length = field.attrs.iter().find_map(|attr| {
            if !attr.path().is_ident("max_length") {
                return None;
            }
            match &attr.meta {
                syn::Meta::NameValue(syn::MetaNameValue {
                    value:
                        syn::Expr::Lit(syn::ExprLit {
                            lit: syn::Lit::Int(literal),
                            ..
                        }),
                    ..
                }) => Some(
                    literal
                        .base10_parse::<u32>()
                        .expect("#[max_length] must be a positive integer."),
                ),
                _ => panic!("#[max_length] must be of the form `#[max_length = ...]`."),
            }
        });

        let column_type = match ty_str {
            "String" => match max_length {
                Some(length) => quote! {
                    sea_query::ColumnType::String(sea_query::StringLen::N(#length))
                },
                None => quote! { dialect.column_type_string() },
            },
            "i64" => quote! { dialect.column_type_i64() },
            "bool" => quote! { dialect.column_type_bool() },
            "DateTime<Utc>" | "UtcDateTime" => {
                quote! { dialect.column_type_date_time_with_time_zone() }
            }
            "Uuid" | "UuidText" => quote! { dialect.column_type_uuid() },
            _ => panic!("#[ducklake_table] does not support dtype {}", ty_str),
        };

        let is_primary_key = field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("primary_key"));
        let is_not_null = field
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("not_null"));
        field.attrs.clear();

        Field {
            snake_name: ident,
            camel_name,
            is_primary_key,
            // NOTE: Unless tagged explicitly with `#[not_null]`, fields in DuckLake are nullable
            //  by default even if client libraries ensure that no NULL values are written.
            is_nullable: !is_not_null,
            column_type,
        }
    }

    fn to_column_def(&self, enum_name: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        let camel_name = &self.camel_name;
        let column_type = &self.column_type;
        let is_nullable = if !self.is_nullable {
            quote! { .not_null() }
        } else {
            quote! {}
        };
        let is_primary_key = if self.is_primary_key {
            quote! { .primary_key() }
        } else {
            quote! {}
        };
        quote! {
            sea_query::ColumnDef::new_with_type(
                #enum_name::#camel_name,
                #column_type
            )
            #is_nullable
            #is_primary_key
        }
    }
}
