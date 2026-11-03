from __future__ import annotations

from typing import TYPE_CHECKING

from .typedefs import QualifiedName

if TYPE_CHECKING:
    from collections.abc import Callable

    import duckdb
    import polars as pl
    import pyarrow as pa

    from ._native import PyView
    from ._storage import StorageOptionSet
    from .ducklake import Ducklake


class View:
    """A DuckLake view.

    A view is a named `SELECT` query stored in the catalog. Views are read-only: they can be
    created, read, and deleted, but not otherwise modified.
    """

    _pyview: PyView
    _ducklake: Ducklake
    _duckdb_connection_fn: Callable[[], duckdb.DuckDBPyConnection]
    _storage_options: StorageOptionSet
    _time_zone: str

    @classmethod
    def _from_pyview(
        cls,
        pyview: PyView,
        ducklake: Ducklake,
        duckdb_connection_fn: Callable[[], duckdb.DuckDBPyConnection],
        storage_options: StorageOptionSet,
        time_zone: str,
    ) -> View:
        view = cls.__new__(cls)
        view._pyview = pyview
        view._ducklake = ducklake
        view._duckdb_connection_fn = duckdb_connection_fn
        view._storage_options = storage_options
        view._time_zone = time_zone
        return view

    # ---------------------------------------- PROPERTIES --------------------------------------- #

    @property
    def name(self) -> QualifiedName:
        """The fully qualified name of the view."""
        return QualifiedName(*self._pyview.name)

    @property
    def sql(self) -> str:
        """The SQL ``SELECT`` query defining the view."""
        return self._pyview.sql

    @property
    def column_aliases(self) -> list[str] | None:
        """The explicit column aliases of the view, if any were provided at creation time."""
        return self._pyview.column_aliases

    @property
    def tags(self) -> dict[str, str]:
        """The tags associated with the view."""
        return self._pyview.tags

    def get_tag(self, key: str) -> str | None:
        """Get a tag by key, or ``None`` if it is absent, without copying all tags."""
        return self._pyview.get_tag(key)

    @property
    def _duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        return self._duckdb_connection_fn()

    # ------------------------------------------------------------------------------------------- #
    #                                            READING                                         #
    # ------------------------------------------------------------------------------------------- #

    # ------------------------------------------ DUCKDB ----------------------------------------- #

    def scan_duckdb(self) -> duckdb.DuckDBPyRelation:
        """Read the full contents of the view as a DuckDB relation.

        Returns:
            The DuckDB relation containing the data.
        """
        return self._duckdb_connection.sql(f"SELECT * FROM {self.name}")

    # ------------------------------------------ POLARS ----------------------------------------- #

    def scan_polars(self) -> pl.LazyFrame:
        """Lazily read the contents of the view as a Polars LazyFrame.

        The view's query is evaluated against the referenced DuckLake tables using a Polars
        :class:`~polars.SQLContext`.

        DuckLake scalar and table macro calls are expanded before execution. Typed macro
        parameters require DuckDB's type binder and are therefore not supported by this Polars
        execution path. Correlated table-macro arguments are also not supported because Polars SQL
        does not currently support lateral joins.

        Note:
            This requires :mod:`polars` to be installed.
        """
        from .polars.scan import scan_view

        return scan_view(self)

    def read_polars(self) -> pl.DataFrame:
        """Read the full contents of the view as a Polars DataFrame.

        Note:
            This requires :mod:`polars` to be installed.
        """
        from .polars.scan import read_view

        return read_view(self)

    # ------------------------------------------ ARROW ------------------------------------------ #

    def read_arrow(self) -> pa.Table:
        """Read the full contents of the view as a PyArrow table.

        Returns:
            The PyArrow table containing the data.

        Note:
            This requires :mod:`pyarrow` and :mod:`duckdb` to be installed.
        """
        return self._duckdb_connection.execute(f"SELECT * FROM {self.name}").to_arrow_table()

    # ------------------------------------------------------------------------------------------- #
    #                                            DUNDER                                           #
    # ------------------------------------------------------------------------------------------- #

    def __repr__(self) -> str:
        return f"View(schema='{self.name.schema}', name='{self.name.name}')"
