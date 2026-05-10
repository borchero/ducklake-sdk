---
name: new-spec-version
description: Update the SDK to be compatible with a new version of the DuckLake specification.
user-invocable: true
disable-model-invocation: true
---

# Context

When adding a new spec version, start by updating the `crates/ducklake/src/spec` folder for the new version.
Afterwards, the remainder of the codebase should be updated s.t. `cargo check` runs successfully.

# Instructions

Follow these steps to add support for a new version of the DuckLake specification in the Rust SDK:

1. Extract the version of the new specification to support from the user query. Add the new version as the last element
   to the `SUPPORTED_SPEC_VERSIONS`.
2. Find the relevant migrations in the official DuckDB ducklake implementation at
   https://raw.githubusercontent.com/duckdb/ducklake/main/src/storage/ducklake_metadata_manager.cpp. The relevant
   migration is the one where `ExecuteMigration` is called with the previous latest supported spec version and the new
   one.
3. Update all entity definitions in `entities.rs` in line with the new specification. This especially includes adding
   new fields, updating existing ones, and adding new entity types if necessary. Make sure that the order of fields
   matches the order in the official implementation.
4. Add a new migration in the `migrations/` folder in a new file matching new name of the new version. The
   implementation of the new migration must be structurally equivalent to the implementation of the existing
   migrations. Do not add comments just like it is the case for existing migrations. Newly created tables MUST NOT be
   created with `Table::create_entity` but with manual `Table::create_entity` calls. Migrations MUST NOT access
   `crate::spec::entities` as this renders migrations useless once the entities are updated.
5. Update `init.rs` with newly available entities and, if necessary, new defaults. Whether the latter is necessary
   depends on whether the contents of `InitializeDuckLake` in
   https://raw.githubusercontent.com/duckdb/ducklake/main/src/storage/ducklake_metadata_manager.cpp differs from what
   is currently implemented.
6. Update the constants in `metadata.rs` to match the descriptions and available options in
   https://raw.githubusercontent.com/duckdb/ducklake/main/src/functions/ducklake_options.cpp. Update the remainder of
   the codebase to support new options for reading and writing global, schema-level, and table-level metadata. Make
   sure to skip experimental options.

Afterwards, work on required updates to the rest of the codebase.
