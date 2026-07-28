==================
Polars Integration
==================

The :mod:`ducklake.polars` module exposes functional helpers that mirror Polars'
own ``sink_*`` and ``scan_*`` APIs. Most users should prefer the methods on
:class:`~ducklake.Table` (e.g. :meth:`~ducklake.Table.sink_polars`,
:meth:`~ducklake.Table.scan_polars`), which call into these helpers internally.

Enums
-----

Polars :class:`polars.datatypes.Enum` columns can be used when creating a table from a
:class:`polars.Schema`:

.. code-block:: python

    import ducklake as dl
    import polars as pl

    lake = dl.create("sqlite:///metadata.sqlite", data_path="data/")
    priority = pl.Enum(["low", "medium", "high"])
    table = lake.create_table(
        "tasks",
        pl.Schema({"name": pl.String, "priority": priority}),
    )

    table.write_polars(
        pl.DataFrame(
            {"name": ["Review", "Release"], "priority": ["medium", "high"]},
            schema_overrides={"priority": priority},
        )
    )

    assert table.read_polars().schema["priority"] == priority

DuckLake stores the values as ``VARCHAR`` and records the ordered categories in a versioned table
tag keyed by stable field ID. Consequently, DuckDB and other DuckLake clients see the original
string labels, while :meth:`~ducklake.Table.scan_polars` and
:meth:`~ducklake.Table.read_polars` restore the logical Enum data type. Struct fields and List
elements containing Enums are supported as well.

The categories are fixed when the table is created. Writes may provide either Enum or String
columns, but every non-null value must belong to the table's category set. Enum filters are
applied after converting the physical String column, so they currently do not benefit from
Parquet predicate pushdown.

.. currentmodule:: ducklake.polars
.. autosummary::
    :toctree: _gen/

    sink_ducklake
