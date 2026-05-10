# `ducklake::catalog`

The `ducklake::catalog` module provides a representation of the catalog in a DuckLake. This includes:

- The currently active schemas
- The tables within those schemas
- The columns in all of those tables, including their data types and constraints
- The partitioning information about tables
- Tags for all entities (schemas, tables, columns)

The catalog is designed to be _mutable_:

- Entities which are added to the catalog (e.g. a new table) are marked as _pending_ until they receive a proper ID
  generated as part of a transaction commit
- Entities which are removed from the catalog are marked as _deleted_ without ever being removed

This allows to easily evolve the catalog within a transaction without the need for a secondary transaction-only catalog
type.

## Architecture

### Names, IDs, References

There are two different levels of abstractions for entity references:

- Users generally interact with entities by name (e.g. table name, column name, ...). However, this is impractical
  within an evolving catalog as entities can be renamed and reference might break.
- Within the DuckLake database, entities have a static ID which fixes the issue of dangling references. However, using
  these IDs _exclusively_ within the catalog is not possible as IDs are pending when new entities are inserted: the IDs
  can only be assigned during a transaction commit.

As a result, the `ducklake::catalog` module introduces the concept of _references_ which are similar to IDs, i.e.
static for an entity, but already exist for pending entities. To reference the different entities, there exists
`SchemaRef`, `TableRef`, and `ColumnRef`.

#### Arena Index

All references are backed by instances of the `ArenaIdx` struct which is used to index into append-only vectors
("arenas") that maintain entities. This type **must not be exposed outside of this module**.

### Public API

The public surface of the catalog is meant to be minimal. None of the internal representations of the catalog should
leak outside of this module (i.e. not even be exposed as `pub(crate)`). This especially includes:

- The internal representation of the entities (e.g. `CatalogTable`, `CatalogColumns`, ...)
- The `CatalogState` enum

All publicly exposed functions must only have parameters and return types of the following kinds:

- Names (e.g. table names) and references (i.e. `TableRef`, ...)
- Crate-level types (e.g. `crate::Table`)
- Collections thereof (e.g. `Vec`)

Functions that allow the modification of internal state (i.e. set the ID of a pending entity) must not return mutable
references to internal types. Instead, they should accept closures that provide values on-demand.
