# `ducklake::db`

The `ducklake::db` module provides a database abstraction layer for interacting with the DuckLake metadata store. It
supports multiple database backends (PostgreSQL, MySQL, SQLite) through a unified API using feature flags.

## Architecture

### Multi-Database Support

The module uses an internal enum pattern to abstract over different database backends:

- `AnyPool` wraps `sqlx::Pool` for each supported database (Postgres, MySQL, SQLite)
- `AnyTransaction` wraps `sqlx::Transaction` for each supported database
- Feature flags (`postgres`, `mysql`, `sqlite`) control which backends are compiled

This design allows the same code to work with any supported database without runtime overhead from dynamic dispatch.

### Public Types

The module exposes two primary types:

- `Pool`: A single-connection pool to the metadata database. Provides methods for querying (`fetch_one`,
  `fetch_optional`, `fetch_all`), executing statements (`execute`), checking table existence (`table_exists`), and
  beginning transactions (`begin`).
- `Transaction`: A database transaction with `REPEATABLE READ` isolation level. Provides methods for executing
  statements and fetching results within the transaction context.

### SQL Generation with sea-query

The module uses `sea-query` for database-agnostic SQL generation:

- The `Dialect` enum represents the active database backend
- The `SqlConvertible` trait converts sea-query statements to dialect-specific SQL with bound parameters
- Both `Pool` and `Transaction` implement `AsRef<Dialect>` to enable dialect-aware SQL generation

### sea-query Extensions

The `sea_query_ext` submodule provides helper traits for common patterns:

- `CreateTable` / `CreatableEntity`: Simplifies table creation by allowing entity types to define their own schema
- `InsertIntoTable` / `InsertableEntity`: Simplifies row insertion by allowing entity types to define their own insert
  logic

These traits are used in conjunction with the `ducklake_macros` crate to derive implementations automatically.

### Row Type Abstraction

The `RowType` trait alias provides a unified bound for types that can be deserialized from database rows. It is defined
conditionally based on enabled features to ensure compatibility with all active backends.

## Design Principles

### Single-Connection Pools

All pools are configured with `max_connections(1)` to ensure serializable access to the metadata store. This simplifies
concurrency handling and matches DuckLake's transaction model.

### Transaction Isolation

Transactions use appropriate isolation levels for each backend:

- PostgreSQL: `REPEATABLE READ`
- MySQL: `REPEATABLE READ`
- SQLite: `BEGIN IMMEDIATE` (provides write-ahead locking)
