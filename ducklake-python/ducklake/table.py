from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Literal, overload

from .typedefs import (
    ArrowStreamExportable,
    Column,
    DataType,
    MaintenanceResult,
    PartitionColumn,
    Partitioning,
    ScanResult,
    Schema,
    TableMetadata,
    TableMetadataUpdate,
    TableName,
    Value,
    WriteDataFile,
    _serialize_metadata_value,
)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    import duckdb
    import polars as pl
    import pyarrow as pa
    from polars._typing import EngineType

    from ._native import PyDataFilePathGenerator, PyTable
    from ._storage import StorageOptionSet


class Table:
    """A DuckLake table."""

    _pytable: PyTable
    _duckdb_connection_fn: Callable[[], duckdb.DuckDBPyConnection]
    _storage_options: StorageOptionSet

    @classmethod
    def _from_pytable(
        cls,
        pytable: PyTable,
        duckdb_connection_fn: Callable[[], duckdb.DuckDBPyConnection],
        storage_options: StorageOptionSet,
    ) -> Table:
        table = cls.__new__(cls)
        table._pytable = pytable
        table._duckdb_connection_fn = duckdb_connection_fn
        table._storage_options = storage_options
        return table

    # ---------------------------------------- PROPERTIES --------------------------------------- #

    @property
    def name(self) -> TableName:
        """The fully qualified name of the table."""
        return TableName(*self._pytable.name)

    @property
    def schema(self) -> Schema:
        """The schema of the table."""
        return Schema(self._pytable.columns)

    @property
    def partitioning(self) -> Partitioning | None:
        """The partitioning of the table, if any."""
        partitioning = self._pytable.partitioning
        if partitioning is None:
            return None
        return Partitioning(
            [
                PartitionColumn(
                    col[0],
                    transform=col[1],  # type: ignore
                    num_buckets=col[2],  # type: ignore
                )
                for col in partitioning
            ]
        )

    @property
    def tags(self) -> dict[str, str]:
        """The tags associated with the table."""
        return dict(self._pytable.tags)

    @property
    def metadata(self) -> TableMetadata:
        """The metadata associated with the table."""
        return self._pytable.metadata

    @property
    def _duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        return self._duckdb_connection_fn()

    # ------------------------------------------------------------------------------------------- #
    #                                        READ AND WRITE                                       #
    # ------------------------------------------------------------------------------------------- #

    def scan(self) -> ScanResult:
        """Scan the table and return all data files with their associated delete files."""
        return self._pytable.scan()

    def _get_write_info(self) -> tuple[TableMetadata, PyDataFilePathGenerator]:
        return self._pytable.get_write_info()

    def _write_data_files(self, new_data_files: list[WriteDataFile]) -> None:
        self._pytable.write_data_files(new_data_files)

    def _write_inline_data(self, data: ArrowStreamExportable) -> None:
        self._pytable.write_inline_data(data)

    # ------------------------------------------ DUCKDB ----------------------------------------- #

    def scan_duckdb(self) -> duckdb.DuckDBPyRelation:
        """Read the full contents of the table as a DuckDB relation.

        Returns:
            The DuckDB relation containing the data.
        """
        return self._duckdb_connection.table(str(self.name))

    # ------------------------------------------ POLARS ----------------------------------------- #

    @overload
    def sink_polars(
        self,
        lf: pl.LazyFrame,
        *,
        engine: EngineType = "auto",
        optimizations: pl.QueryOptFlags | None = None,
        lazy: bool = False,
    ) -> None: ...

    @overload
    def sink_polars(
        self,
        df: pl.DataFrame,
        *,
        engine: EngineType = "auto",
        optimizations: pl.QueryOptFlags | None = None,
        lazy: Literal[True],
    ) -> pl.LazyFrame: ...

    def sink_polars(
        self,
        lf: pl.LazyFrame,
        *,
        engine: EngineType = "auto",
        optimizations: pl.QueryOptFlags | None = None,
        lazy: bool = False,
    ) -> pl.LazyFrame | None:
        from .polars.sink import sink_ducklake

        return sink_ducklake(
            lf,
            self,
            engine=engine,
            optimizations=optimizations,
            lazy=lazy,
        )

    def write_polars(
        self,
        df: pl.DataFrame,
    ) -> None:
        from .polars.sink import write_ducklake

        write_ducklake(df, self)

    def scan_polars(self, *, include_file_paths: str | None = None) -> pl.LazyFrame:
        from .polars.scan import scan_ducklake

        return scan_ducklake(self, include_file_paths=include_file_paths)

    def read_polars(self, *, include_file_paths: str | None = None) -> pl.DataFrame:
        from .polars.scan import read_ducklake

        return read_ducklake(self, include_file_paths=include_file_paths)

    # ------------------------------------------ ARROW ------------------------------------------ #

    def write_arrow(self, data: pa.Table) -> None:
        """Append the provided data to the table.

        Args:
            data: The PyArrow table containing the data to append. The schema of the data must
                match the table's current schema.

        Note:
            This requires :mod:`pyarrow` and :mod:`duckdb` to be installed.
        """
        column_names = [col.name for col in self.schema.columns]
        joined_column_names = ", ".join(column_names)
        self._duckdb_connection.execute(
            f"INSERT INTO {self.name} ({joined_column_names}) "
            f"SELECT {joined_column_names} FROM data"
        )

    def read_arrow(self) -> pa.Table:
        """Read the full contents of the table as a PyArrow table.

        Returns:
            The PyArrow table containing the data.

        Note:
            This requires :mod:`pyarrow` and :mod:`duckdb` to be installed.
        """
        return self._duckdb_connection.execute(f"SELECT * FROM {self.name}").to_arrow_table()

    # ------------------------------------------------------------------------------------------- #
    #                                        SCHEMA UPDATES                                       #
    # ------------------------------------------------------------------------------------------- #

    def rename(self, new_name: str) -> None:
        """Rename the table in the catalog.

        Args:
            new_name: The new name for the table.

        Note:
            This operation does not affect the schema the table resides in. It is not currently
            possible to move a table to a different schema.
        """
        self._pytable.rename(new_name)

    def update_partitioning(self, partitioning: Partitioning | None) -> None:
        """Update the partitioning of this table.

        Args:
            partitioning: The new partitioning. If `None` is provided, the partitioning of the
                table is reset.

        Note:
            This is a metadata-only operation which does not rewrite data files. As a result,
            queries might not be fully optimized.
        """
        self._pytable.update_partitioning(
            None
            if partitioning is None
            else [(col.name, col.transform, col.num_buckets) for col in partitioning.columns]
        )

    def add_column(self, column: Column) -> None:
        """Add a new column to the table.

        Args:
            column: The column to add.
        """
        self._pytable.add_column(column)

    def rename_column(self, column: str, new_name: str) -> None:
        """Rename a column in the table.

        Args:
            column: The current name of the column to rename.
            new_name: The new name for the column.
        """
        self._pytable.rename_column(column, new_name)

    def remove_column(self, column: str) -> None:
        """Remove a column from the table.

        Args:
            column: The name of the column to remove.
        """
        self._pytable.remove_column(column)

    def update_column_dtype(self, column: str, new_dtype: DataType) -> None:
        """Update the data type of the provided column.

        Generally speaking, data types can only be changed via type promotion. For example,
        integers can be turned into larger integers.

        For struct columns, updating the data type allows adding and dropping fields.

        Args:
            column: The column for which to change the data type.
            new_dtype: The new data type of the column.
        """
        self._pytable.update_column_dtype(column, new_dtype)

    def update_column_default(
        self, column: str, default_value: Value | tuple[str, str] | None = None
    ) -> None:
        """Update the default value of a column.

        The default value can be a literal value, or an expression specified as a
        `(dialect, expression)` tuple. Pass `None` to remove the default.

        Args:
            column: The column for which to change the default value.
            default_value: The new default value.
        """
        self._pytable.update_column_default(column, default_value)

    def update_column_nullability(self, column: str, nullable: bool) -> None:
        """Update the nullability of a column.

        Args:
            column: The column for which to change the nullability.
            nullable: Whether the column should allow null values.
        """
        self._pytable.update_column_nullability(column, nullable)

    def update_schema(self, schema: Schema | Sequence[Column] | Mapping[str, DataType]) -> None:
        """Update the full schema of the table.

        This is a convenience function that allows to easily add and remove multiple columns as
        well as changing the data type of existing columns.

        Args:
            schema: The new schema of the table.
        """
        schema_cls = schema if isinstance(schema, Schema) else Schema(schema)
        self._pytable.update_schema(schema_cls.columns)

    def delete(self) -> None:
        """Delete the table from the catalog.

        After calling this method, the Table object is no longer valid.
        """
        self._pytable.delete()

    def add_tag(self, key: str, value: str) -> None:
        """Add a new tag to the table.

        Args:
            key: The key of the tag.
            value: The value of the tag.
        """
        self._pytable.add_tag(key, value)

    def remove_tag(self, key: str) -> None:
        """Remove an existing tag from the table.

        Args:
            key: The key of the tag.

        Raises:
            ValueError: If no tag for the provided key exists.
        """
        self._pytable.remove_tag(key)

    def add_column_tag(self, column: str | Sequence[str], key: str, value: str) -> None:
        """Add a new tag to a column.

        Args:
            column: The name of the column to add the tag to. This may be provided as a "path"
                to a nested column.
            key: The key of the tag.
            value: The value of the tag.
        """
        self._pytable.add_column_tag(
            column if isinstance(column, str) else list(column),
            key,
            value,
        )

    def remove_column_tag(self, column: str | Sequence[str], key: str) -> None:
        """Remove an existing tag from a column.

        Args:
            column: The name of the column to remove the tag from. This may be provided as a
                "path" to a nested column.
            key: The key of the tag.

        Raises:
            ValueError: If no tag for the provided key exists.
        """
        self._pytable.remove_column_tag(
            column if isinstance(column, str) else list(column),
            key,
        )

    def set_metadata(self, **options: Unpack[TableMetadataUpdate]) -> None:
        """Set one or more metadata options for this table.

        Provide options as keyword arguments. Pass `None` as a value to remove the option
        from the metadata (i.e. revert it to its default).

        Raises:
            ValueError: If a key is read-only and cannot be set.

        See also:
            :meth:`Ducklake.set_metadata` for setting metadata options at the global or schema
            scope.
        """
        for key, value in options.items():
            self._pytable.set_metadata(key, _serialize_metadata_value(value))

    # ------------------------------------------------------------------------------------------- #
    #                                         MAINTENANCE                                         #
    # ------------------------------------------------------------------------------------------- #

    def merge_adjacent_files(
        self,
        *,
        max_compacted_files: int | None = None,
        min_file_size: int | None = None,
        max_file_size: int | None = None,
    ) -> list[MaintenanceResult]:
        """Merge small adjacent data files of this table into larger ones.

        Dispatches to `ducklake_merge_adjacent_files` scoped to this table.

        Args:
            max_compacted_files: Maximum number of compaction operations produced in a single
                call.
            min_file_size: Excludes files smaller than this many bytes from compaction.
            max_file_size: Excludes files at or larger than this many bytes from compaction.
                Defaults to the `target_file_size` table option.

        Returns:
            A row for each output file created by the operation.

        Note:
            This requires :mod:`duckdb` to be installed.
        """
        from .duckdb.utils import (
            build_named_query_params,
            fetch_result_dicts,
            parse_maintenance_result,
        )

        params, args = build_named_query_params(
            max_compacted_files=max_compacted_files,
            min_file_size=min_file_size,
            max_file_size=max_file_size,
        )
        return parse_maintenance_result(
            fetch_result_dicts(
                self._duckdb_connection,
                f"CALL ducklake_merge_adjacent_files('my_ducklake', ?, schema => ?{params});",
                [self.name.name, self.name.schema, *args],
            )
        )

    def rewrite_data_files(
        self, *, delete_threshold: float | None = None
    ) -> list[MaintenanceResult]:
        """Rewrite data files of this table that have a high fraction of deleted rows.

        Dispatches to `ducklake_rewrite_data_files` scoped to this table.

        Args:
            delete_threshold: Minimum fraction (0-1) of deleted rows required to trigger a
                rewrite. Defaults to the `rewrite_delete_threshold` metadata option (0.95).

        Returns:
            A row for each output file created by the operation.

        Note:
            This requires :mod:`duckdb` to be installed.
        """
        from .duckdb.utils import (
            build_named_query_params,
            fetch_result_dicts,
            parse_maintenance_result,
        )

        params, args = build_named_query_params(delete_threshold=delete_threshold)
        return parse_maintenance_result(
            fetch_result_dicts(
                self._duckdb_connection,
                f"CALL ducklake_rewrite_data_files('my_ducklake', ?, schema => ?{params});",
                [self.name.name, self.name.schema, *args],
            )
        )

    # ------------------------------------------------------------------------------------------- #
    #                                            DUNDER                                           #
    # ------------------------------------------------------------------------------------------- #

    def __repr__(self) -> str:
        return f"Table(schema='{self.name.schema}', name='{self.name.name}')"
