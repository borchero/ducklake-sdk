=============
API Reference
=============

.. grid::

    .. grid-item-card:: Connecting
        :link: connect/index
        :link-type: doc

        Functions to create or connect to a DuckLake catalog.

    .. grid-item-card:: Ducklake
        :link: ducklake/index
        :link-type: doc

        The main entry point representing a DuckLake instance.

    .. grid-item-card:: Table
        :link: table/index
        :link-type: doc

        Read and write data, inspect metadata, and manage table state.

.. grid::

    .. grid-item-card:: Transactions
        :link: transaction/index
        :link-type: doc

        Group multiple operations into atomic transactions.

    .. grid-item-card:: Schemas & Data Types
        :link: types/index
        :link-type: doc

        Describe table schemas with `ducklake`'s data type primitives.

    .. grid-item-card:: Errors
        :link: errors/index
        :link-type: doc

        Exceptions raised by the SDK.

.. grid::

    .. grid-item-card:: Polars Integration
        :link: polars/index
        :link-type: doc

        Functional helpers for using `ducklake` with Polars.

    .. grid-item-card:: DuckDB Integration
        :link: duckdb/index
        :link-type: doc

        Functional helpers for using `ducklake` with DuckDB.

.. toctree::
    :maxdepth: 1
    :hidden:

    connect/index
    ducklake/index
    table/index
    transaction/index
    types/index
    errors/index
    polars/index
    duckdb/index
