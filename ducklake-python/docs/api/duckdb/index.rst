==================
DuckDB Integration
==================

Most users should prefer the methods on :class:`~ducklake.Table` (e.g.
:meth:`~ducklake.Table.scan_duckdb`, :meth:`~ducklake.Table.write_arrow`),
which interact with DuckDB internally.

The :mod:`ducklake.duckdb.utils` module exposes a few low-level helpers used by
the SDK to interact with DuckDB connections directly.

.. currentmodule:: ducklake.duckdb.utils
.. autosummary::
    :toctree: _gen/

    fetch_result_dicts
    build_named_query_params
    parse_cleanup_path_result
    parse_maintenance_result
