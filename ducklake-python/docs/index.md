# DuckLake Python SDK

The `ducklake` Python package provides a Pythonic interface for interacting with [DuckLake](https://ducklake.select) —
an open table format for lakehouses, backed by a SQL catalog database and Parquet files in object storage.

It is built on top of a Rust implementation of the DuckLake specification and exposes first-class integrations with
[DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [Apache Arrow](https://arrow.apache.org).

```{warning}
This is *not* an official SDK released by the DuckDB Foundation.
The `ducklake` name on PyPI was reserved by the DuckDB Foundation, hence the package is
distributed as `ducklake-sdk` on PyPI but importable as `ducklake`.
```

## Installation

Install the package from PyPI using your favorite package manager:

```bash
pip install ducklake-sdk
pixi add ducklake-sdk
```

To enable the optional integrations, install one of the extras:

```bash
pip install "ducklake-sdk[arrow]"     # Arrow + DuckDB based reads/writes
pip install "ducklake-sdk[polars]"    # Polars based reads/writes
pip install "ducklake-sdk[sqlalchemy]" # SQLAlchemy URLs as catalog connection strings
```

## Quick Example

```python
import ducklake as dl
import polars as pl

# Create a new DuckLake with local storage
ducklake = dl.create("sqlite:///metadata.sqlite", data_path="data_files/")

# Create a new table
table = ducklake.create_table(
    "test",
    schema={"i": dl.Int64(), "s": dl.Varchar()},
)

# Insert data into the table
lf = pl.LazyFrame({"i": [1, 2, 3], "s": ["hello", "ducklake", "sdk"]})
table.sink_polars(lf)

# Read data from the table
lf = table.scan_polars()
```

```{toctree}
:maxdepth: 2
:hidden:

guides/index
api/index
```
