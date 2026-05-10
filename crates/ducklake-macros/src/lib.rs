mod ducklake_table;
mod visibility;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn visibility_if(args: TokenStream, input: TokenStream) -> TokenStream {
    visibility::visibility_if_impl(args, input)
}

#[proc_macro_attribute]
pub fn ducklake_table(_args: TokenStream, input: TokenStream) -> TokenStream {
    ducklake_table::ducklake_table_impl(input)
}
