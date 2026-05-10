# ducklake

An async, standalone Rust SDK for [DuckLake](https://ducklake.select).

> **Status:** Alpha. Expect occasional breaking changes until the full specification is implemented and known
> limitations are addressed.
>
> This is _not_ an official SDK released by the DuckDB Foundation.

## Installation

```bash
cargo add ducklake
```

By default, only the SQLite catalog backend is enabled. To use a different catalog database or a cloud storage backend,
enable the relevant feature flags (see [Cargo features](#cargo-features) below).

## Quick Start

```rust,no_run
use ducklake::{Column, CreateOptions, DataType, Ducklake};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new DuckLake backed by SQLite metadata and local Parquet storage.
    let ducklake = Ducklake::create(
        CreateOptions::new("sqlite://metadata.sqlite", "data_files/"),
    )
    .await?;

    // Create a schema and a table.
    ducklake.create_schema("main", None).await?;
    ducklake
        .create_table(
            "main.events",
            vec![
                Column::new("id".into(), DataType::Int64),
                Column::new("message".into(), DataType::Varchar),
            ],
            None,
            None,
            None,
        )
        .await?;

    Ok(())
}
```

For the complete API reference, see the [documentation](https://docs.rs/ducklake). For end-to-end examples (including
writing and scanning data via Polars or DuckDB through the Python bindings), see the
[main repository](https://github.com/borchero/ducklake-sdk).

## Minimum Supported Rust Version

This crate currently requires a **nightly** Rust toolchain (it relies on the `trait_alias` feature). The pinned
toolchain is tracked in
[`rust-toolchain.toml`](https://github.com/borchero/ducklake-sdk/blob/main/rust-toolchain.toml).
