use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro::TokenStream;
use quote::{format_ident, quote};

pub fn ducklake_table_impl(input: TokenStream) -> TokenStream {
    let mut ast = syn::parse(input).unwrap();
    let ducklake_table = ducklake_table_blocks(&mut ast);
    let result = quote! {
        #[derive(sqlx::FromRow, Debug, Clone)]
        #ast
        #ducklake_table
    };
    result.into()
}

fn ducklake_table_blocks(ast: &mut syn::DeriveInput) -> proc_macro2::TokenStream {
    let visibility = &ast.vis;
    let camel_name = &ast.ident;
    let snake_name_str = camel_name.to_string().to_snake_case();
    let snake_name = format_ident!("{}", snake_name_str);

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

    quote! {
        #visibility mod #snake_name {
            #[derive(sea_query::Iden)]
            #[iden = #snake_name_str]
            pub struct Table;

            #[derive(sea_query::Iden, strum::EnumIter, Clone, Copy, PartialEq, Eq)]
            pub enum Column {
                #(#column_camel_names,)*
            }

            impl Column {
                pub fn col(self) -> sea_query::Expr {
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
            fn insert_into_table(&self) -> sea_query::InsertStatement {
                sea_query::Query::insert()
                    .into_table(#snake_name::Table)
                    .columns([#(#snake_name::Column::#column_camel_names,)*])
                    .values_panic([#(self.#column_snake_names.clone().into(),)*])
                    .to_owned()
            }

            fn insert_all_into_table(
                entities: impl IntoIterator<Item = Self>,
            ) -> sea_query::InsertStatement {
                let mut query = sea_query::Query::insert();
                query
                    .into_table(#snake_name::Table)
                    .columns([#(#snake_name::Column::#column_camel_names,)*]);
                for entity in entities {
                    query.values_panic([#(entity.#column_snake_names.clone().into(),)*]);
                }
                query.to_owned()
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
        let column_type = match ty_str {
            "String" => quote! { dialect.column_type_string() },
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
