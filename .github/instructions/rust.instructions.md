---
applyTo: crates/ducklake/**
---

# DuckLake Rust Crate

This document describes the structure and core design principles for the `ducklake` Rust crate.

The `ducklake` crate is the core Rust implementation for interacting with DuckLake catalogs. It provides an async API
for managing schemas, tables, and transactions against PostgreSQL, MySQL, or SQLite metadata databases.

## Core Design Principles

### 1. Async-First API

All database operations are async using `tokio`:

```rust
impl Ducklake {
    pub async fn create(url: &str, config: InitConfig) -> DucklakeResult<Self> { ... }
    pub async fn connect(url: &str) -> DucklakeResult<Self> { ... }
    pub async fn transaction(&self) -> DucklakeResult<Transaction<'_>> { ... }
}
```

### 2. Multi-Database Support via Feature Flags

The crate supports multiple database backends through feature flags:

- `postgres` - PostgreSQL support
- `mysql` - MySQL support
- `sqlite` - SQLite support

The `db::Pool` type abstracts over these backends using an internal enum:

```rust
enum AnyPool {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Pool<sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::Pool<sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Pool<sqlx::Sqlite>),
}
```

### 3. Arena-Based Catalog

The `Catalog` uses an arena allocation pattern for efficient entity management:

- Entities (schemas, tables) are stored in a `Vec<CatalogEntity>` arena
- References (`SchemaRef`, `TableRef`) are lightweight index wrappers
- Enables efficient cloning and modification during transactions

```rust
struct ArenaIdx(usize);

pub struct Catalog {
    arena: Vec<CatalogEntity>,
    by_id: HashMap<i64, ArenaIdx>,
    schemas: HashMap<String, ArenaIdx>,
}
```

### 4. Copy-on-Write Catalog Sharing

The `CowArc<T>` utility enables efficient catalog sharing across transactions:

- Multiple readers share the same `Arc<Catalog>`
- Writers clone on first mutation via `Arc::make_mut`
- Minimizes memory usage for read-heavy workloads

### 5. Transaction Change Tracking

Transactions track changes through a `ChangeSet` containing `Change` variants:

- `CreateSchema`, `DeleteSchema`
- `CreateTable`, `DeleteTable`, `AlterTable`
- Changes are applied atomically on commit

The catalog maintains entity state during transactions:

```rust
enum CatalogState {
    Existing { id: i64 },  // Entity exists in database
    Pending,               // Created in this transaction
    Deleted { id: i64 },   // Marked for deletion
}
```

### 6. Borrowed Lifetime Abstraction

The `Borrowed<'a, T>` type provides zero-cost lifetime abstraction:

- Without `python` feature: simple `&'a T` reference
- With `python` feature: `Cow<'a, T>` that can be owned

This enables the same code to work with both Rust lifetimes and Python bindings:

```rust
pub struct Transaction<'a> {
    pool: Borrowed<'a, db::Pool>,
    // ...
}

#[cfg(feature = "python")]
impl<'a> Transaction<'a> {
    pub fn into_owned(self) -> Transaction<'static> { ... }
}
```

### 7. sea-query for SQL Generation

The crate uses `sea-query` for database-agnostic SQL generation:

- Entity tables defined using `Iden` derive macro
- Queries built programmatically and converted to dialect-specific SQL
- Custom extensions in `sea_query_ext.rs` for common patterns

### 8. Error Handling

All fallible operations return `DucklakeResult<T>`:

```rust
pub type DucklakeResult<T> = Result<T, DucklakeError>;

#[derive(thiserror::Error, Debug)]
pub enum DucklakeError {
    #[error("{entity} with name '{name}' already exists")]
    AlreadyExists { entity: &'static str, name: String },
    // ...
}
```

Helper methods provide convenient error construction:

```rust
impl DucklakeError {
    pub fn schema_already_exists(name: &str) -> Self { ... }
    pub fn table_not_found(name: &TableName) -> Self { ... }
}
```

## Public API

The crate exports a minimal public API in `lib.rs`:

```rust
// Main types
pub use ducklake::Ducklake;
pub use error::DucklakeError;
pub use spec::InitConfig as DucklakeInitConfig;
pub use transaction::{AuthorInfo, Transaction};

// Table types
pub use table::{Column, DataType, PartitionColumn, Table, TimestampPrecision};

// Type definitions
pub use typedefs::*;  // SchemaName, TableName, Tag, etc.
```

Internal types use `pub(crate)` visibility.

## Code Style Guidelines

### Naming Conventions

- Types: `PascalCase` (`Ducklake`, `Transaction`, `CatalogSchema`)
- Functions/methods: `snake_case` (`create_schema`, `get_table`)
- Constants: `SCREAMING_SNAKE_CASE` (`SUPPORTED_VERSIONS`)
- Private fields: no prefix, use `pub(crate)` for crate-internal access

### Module Organization

- One primary type per file when the type is complex
- Related small types can share a file
- Use `mod.rs` for module roots with re-exports
- Keep `mod.rs` files focused on exports and high-level structure

### Documentation

- Document all public items with `///` doc comments
- Include `# Arguments`, `# Errors`, and `# Examples` sections where appropriate
- Use `# Panics` section if the function can panic

### Error Handling

- Return `DucklakeResult<T>` for fallible operations
- Use `?` operator for error propagation
- Provide context in error messages
- Use `thiserror` for error type definitions
