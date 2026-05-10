# Quickstart

This guide walks through the typical workflow of creating a DuckLake, writing data into a table, and reading it back.

## Connecting to a DuckLake

A DuckLake consists of a _catalog database_ (which stores metadata) and a _data path_ (which stores the actual data
files). The catalog database can be PostgreSQL, MySQL, or SQLite. The data path can be a local directory or a cloud
storage URI such as `s3://my-bucket/data`.

To create a brand new DuckLake, use {func}`ducklake.create`:

```python
import ducklake as dl

ducklake = dl.create(
    "sqlite:///metadata.sqlite",
    data_path="data_files/",
)
```

To connect to an existing DuckLake, use {func}`ducklake.connect`:

```python
ducklake = dl.connect("sqlite:///metadata.sqlite")
```

When connecting to remote object storage, you may pass storage credentials via the `storage_options` argument:

```python
ducklake = dl.connect(
    "postgresql://user:password@localhost:5432/catalog",
    storage_options={
        "aws_region": "us-east-1",
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
    },
)
```

## Creating a Table

Tables are created from the {class}`~ducklake.Ducklake` instance. The schema is described using `ducklake`'s data type
primitives:

```python
table = ducklake.create_table(
    "houses",
    schema={
        "zip_code": dl.Varchar(),
        "num_bedrooms": dl.UInt8(),
        "num_bathrooms": dl.UInt8(),
        "price": dl.Float64(),
    },
)
```

You can also pass a list of {class}`~ducklake.Column` instances to gain fine-grained control over nullability,
defaults, and tags:

```python
table = ducklake.create_table(
    "houses",
    schema=[
        dl.Column("zip_code", dl.Varchar(), nullable=False),
        dl.Column("num_bedrooms", dl.UInt8(), nullable=False),
        dl.Column("num_bathrooms", dl.UInt8(), nullable=False),
        dl.Column("price", dl.Float64(), nullable=False),
    ],
)
```

## Writing Data

Data can be written into a table using one of the framework integrations. For Polars users, the
{meth}`~ducklake.Table.sink_polars` method takes any `pl.LazyFrame`:

```python
import polars as pl

lf = pl.LazyFrame(
    {
        "zip_code": ["01234", "01234", "12345"],
        "num_bedrooms": [2, 3, 1],
        "num_bathrooms": [1, 2, 1],
        "price": [100_000.0, 250_000.0, 75_000.0],
    }
)
table.sink_polars(lf)
```

For DuckDB users, you can write Arrow-compatible objects via {meth}`~ducklake.Table.write` or pass relations directly.
See the [API reference](../api/table/index) for the full set of methods.

## Reading Data

Reading mirrors the writing API. To get a Polars `LazyFrame` over the table contents:

```python
lf = table.scan_polars()
df = lf.collect()
```

`ducklake` automatically applies any pending deletion files and inline deletions, so you always see a consistent view
of the table.

## Time Travel

DuckLake snapshots are first-class citizens. To read the table as it looked at a previous point in time, time-travel
the entire {class}`~ducklake.Ducklake` connection:

```python
import datetime as dt

# Time travel by snapshot ID
old = ducklake.at(42)

# Time travel by timestamp
yesterday = ducklake.at(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1))

old_table = old.table("houses")
df = old_table.read_polars()
```

## Transactions

Multiple metadata changes can be grouped into a single atomic transaction using {meth}`~ducklake.Ducklake.transaction`:

```python
with ducklake.transaction() as tx:
    tx.create_schema("analytics")
    tx.create_table(
        ("analytics", "events"),
        schema={"id": dl.Int64(), "name": dl.Varchar()},
    )
```

If the `with` block raises an exception, the transaction is rolled back automatically. Otherwise, all changes are
committed atomically when the block exits.

## Next Steps

- Explore the [API Reference](../api/index.rst) for details on all available classes and methods.
- Read the upstream [DuckLake documentation](https://ducklake.select/docs/stable) to learn about the underlying format.
