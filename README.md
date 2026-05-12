<div align="center">

  <h3 align="center">
  <code>ducklake-sdk</code> — Native SDKs for <a href="https://ducklake.select">DuckLake</a>
  </h3>

  <p align="center">
    Read and write DuckLake tables from Rust and Python - no DuckDB required.
  </p>

[![CI](https://img.shields.io/github/actions/workflow/status/borchero/ducklake-sdk/ci.yml?style=flat-square&branch=main&label=CI)](https://github.com/borchero/ducklake-sdk/actions/workflows/ci.yml)
[![Build](https://img.shields.io/github/actions/workflow/status/borchero/ducklake-sdk/build.yml?style=flat-square&branch=main&label=Build)](https://github.com/borchero/ducklake-sdk/actions/workflows/build.yml)
[![GitHub Release](https://img.shields.io/github/v/release/borchero/ducklake-sdk?style=flat-square&label=Release)](https://github.com/borchero/ducklake-sdk/releases)

</div>

---

DuckLake is an integrated data lake and catalog format that stores metadata in a SQL catalog database and writes data
as Parquet files. This repository provides standalone Rust and Python SDKs that talk to DuckLakes directly, with no
dependency on DuckDB or its DuckLake extension.

All language SDKs are built on the same Rust core, which bundles the implementation of the DuckLake specification.

**Python ([`ducklake-sdk`](https://pypi.org/project/ducklake-sdk))**

[![pypi-version](https://img.shields.io/pypi/v/ducklake-sdk.svg?logoColor=white&logo=pypi&style=flat-square&label=pypi)](https://pypi.org/project/ducklake-sdk)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/ducklake-sdk?logoColor=white&logo=conda-forge&style=flat-square)](https://prefix.dev/channels/conda-forge/packages/ducklake-sdk)
[![python-version](https://img.shields.io/pypi/pyversions/ducklake-sdk?logoColor=white&logo=python&style=flat-square)](https://pypi.org/project/ducklake-sdk)
[![readthedocs](https://img.shields.io/readthedocs/ducklake-sdk?logo=readthedocs&logoColor=white&style=flat-square&label=docs)](https://ducklake-sdk.readthedocs.io)
[![codecov](https://codecov.io/gh/borchero/ducklake-sdk/graph/badge.svg?component=python&token=A2h8NFb4Bx)](https://codecov.io/gh/borchero/ducklake-sdk/tree/main?components%5B0%5D=Python)

**Rust ([`ducklake`](https://crates.io/crates/ducklake))**

[![crates.io](https://img.shields.io/crates/v/ducklake.svg?logo=rust&logoColor=white&style=flat-square&label=crates.io)](https://crates.io/crates/ducklake)
[![docs.rs](https://img.shields.io/docsrs/ducklake?logo=docsdotrs&logoColor=white&style=flat-square&label=docs.rs)](https://docs.rs/ducklake)
[![codecov](https://codecov.io/gh/borchero/ducklake-sdk/graph/badge.svg?component=rust&token=A2h8NFb4Bx)](https://codecov.io/gh/borchero/ducklake-sdk/tree/main?components%5B0%5D=Rust)

<!-- prettier-ignore -->
> [!WARNING]
> This is _not_ an official SDK released by the DuckDB Foundation.

## Getting Started

**Python**

```bash
pip install ducklake-sdk            # core
pip install "ducklake-sdk[polars]"  # Polars integration
pip install "ducklake-sdk[arrow]"   # Arrow + DuckDB integration
```

**Rust**

```bash
cargo add ducklake
```

### Quick Example

```python
import ducklake as dl
import polars as pl

# Create a new DuckLake backed by SQLite metadata and local Parquet storage
ducklake = dl.create("sqlite:///metadata.sqlite", data_path="data_files/")

# Define a table.
table = ducklake.create_table(
    "events",
    schema={"id": dl.Int64(), "message": dl.Varchar()},
)

# Write data using Polars
lf = pl.LazyFrame({"id": [1, 2, 3], "message": ["hello", "ducklake", "sdk"]})
table.sink_polars(lf)

# Read it back as a Polars LazyFrame
df = table.scan_polars().collect()
```

For the full API, see the [Python documentation](https://ducklake-sdk.readthedocs.io) or the
[Rust API docs](https://docs.rs/ducklake).

## Features

The Rust core — and therefore every SDK built on top of it — supports:

- **Metadata operations** — schemas, tables,
  [schema evolution](https://ducklake.select/docs/stable/duckdb/usage/schema_evolution),
  [partitioning](https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning),
  [constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints), and
  [table/column tags](https://ducklake.select/docs/stable/duckdb/advanced_features/comments)
- **[Transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)** with
  [conflict resolution](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution)
- **[Data inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining)** for small writes
- **[Metadata configuration](https://ducklake.select/docs/stable/duckdb/usage/configuration)**
- **[Time travel](https://ducklake.select/docs/stable/duckdb/usage/time_travel)** queries

The Python SDK additionally provides:

- Reading and writing data through [Polars](https://github.com/pola-rs/polars)
- Reading, writing, and deleting data through [DuckDB](https://github.com/duckdb/duckdb)
- [Maintenance operations](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance) —
  compaction, snapshot expiration, and more — via DuckDB

### Compatibility Matrix

**Catalog Databases**

| Database | Status                  |
| -------- | ----------------------- |
| SQLite   | ✅                      |
| Postgres | ✅                      |
| MySQL    | 🟧 (no data inlining\*) |

<sub>\*Data inlining for MySQL is not defined in the DuckLake specification.</sub>

**Storage Backends**

| Backend              | Status |
| -------------------- | ------ |
| Local / NFS          | ✅     |
| AWS S3-compatible    | ✅     |
| Google Cloud Storage | ❌     |
| Azure Blob Storage   | ❌     |

**DuckLake Specification Versions**

| Version | Status                  |
| ------- | ----------------------- |
| 1.0     | ✅ (actively supported) |
| 0.4     | ⬆️ (requires migration) |
| 0.3     | ⬆️ (requires migration) |
| 0.2     | ⬆️ (requires migration) |
| 0.1     | ⬆️ (requires migration) |

See the DuckLake [release calendar](https://ducklake.select/release_calendar) for upcoming versions.

## Project Status

<!-- prettier-ignore -->
> [!NOTE]
> This project is in **alpha**. It will move to beta once the full specification is implemented, and to stable once
> all relevant limitations have been addressed. Expect occasional breaking changes until then.

### Not yet implemented from the specification

- [ ] `GEOMETRY` and `VARIANT` data types
- [ ] Mapping columns by name (Parquet files must currently carry field IDs)
- [ ] Views, macros, sort info, and encrypted files

### Known limitations

**Rust SDK** (may impact efficiency):

- [ ] Tables partitioned with a non-identity transform do not benefit from file pruning yet.
- [ ] Filters are not pushed down into the metadata query. Statistics are still loaded eagerly and used by readers to
      prune files, but the metadata query may transmit more data than necessary.
- [ ] Not tested on Windows.

**Python SDK**:

- [ ] Maintenance operations (compaction, snapshot expiration, ...) are dispatched to DuckDB rather than implemented
      natively.
- [ ] Performance of polars reads and writes can be optimized further:
  - Writes currently require reading the file footer after the file has already been written (see also
    https://github.com/pola-rs/polars/issues/27226)
  - Reads currently suffer from suboptimal footer reads (see also https://github.com/pola-rs/polars/issues/27227)

## Contributing

Contributions, bug reports, and feature requests are very welcome. See the
[contribution guidelines](.github/CONTRIBUTING.md) to get started.

## License

Licensed under the [MIT License](LICENSE).
