from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

from .typedefs import (
    Column,
    DataType,
    PartitionColumn,
    Partitioning,
    Schema,
    TableMetadata,
    TableName,
    Value,
    WriteDataFile,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    import polars as pl
    from polars._typing import EngineType

    from ._native import PyDataFilePathGenerator, PyTransaction, PyTransactionTable
    from ._storage import StorageOptionSet
    from .typedefs import ArrowStreamExportable


class Transaction:
    _pytx: PyTransaction
    _storage_options: StorageOptionSet

    @classmethod
    def _from_pytransaction(
        cls, pytransaction: PyTransaction, storage_options: StorageOptionSet
    ) -> Transaction:
        transaction = cls.__new__(cls)
        transaction._pytx = pytransaction
        transaction._storage_options = storage_options
        return transaction

    def create_schema(
        self,
        name: str,
        *,
        data_path: str | None = None,
        if_exists: Literal["fail", "skip"] = "fail",
    ) -> None:
        self._pytx.create_schema(name, data_path, if_exists)

    def delete_schema(self, name: str) -> None:
        self._pytx.delete_schema(name)

    def list_schemas(self) -> list[str]:
        """List all schemas in the transaction-local catalog."""
        return self._pytx.list_schemas()

    def list_tables(self, schema: str | None = None) -> list[TransactionTable]:
        """List all tables in the transaction-local catalog.

        Args:
            schema: Optional schema name to filter tables by. If None, returns all tables
                across all schemas.

        Returns:
            A list of transaction table objects, optionally filtered by schema.
        """
        return [
            TransactionTable._from_pytransaction_table(table, self._storage_options)
            for table in self._pytx.list_tables(schema)
        ]

    def table(self, name: str | TableName) -> TransactionTable:
        pytransaction_table = self._pytx.table(name)
        return TransactionTable._from_pytransaction_table(
            pytransaction_table, self._storage_options
        )

    def create_table(
        self,
        name: str | tuple[str, str] | TableName,
        schema: Schema | Sequence[Column] | Mapping[str, DataType],
        *,
        partition_by: (
            Partitioning | Sequence[PartitionColumn] | Sequence[str] | PartitionColumn | str | None
        ) = None,
        data_path: str | None = None,
        tags: Mapping[str, str] | None = None,
        if_exists: Literal["fail", "skip"] = "fail",
    ) -> TransactionTable:
        """Create a new table as part of this transaction.

        The table (and any subsequent changes or writes against it) only becomes visible once
        the transaction is committed.

        Args:
            name: The fully qualified name of the new table.
            schema: The schema of the new table.
            partition_by: Optional partitioning for the table.
            data_path: Optional data path for the table.
            tags: Optional tags to attach to the table.

        Returns:
            A :class:`TransactionTable` referring to the newly created table.
        """
        schema_cls = schema if isinstance(schema, Schema) else Schema(schema)
        partition_cls = (
            partition_by
            if isinstance(partition_by, Partitioning)
            else (Partitioning(partition_by) if partition_by is not None else None)
        )
        pytransaction_table = self._pytx.create_table(
            name,
            schema_cls.columns,
            partition=(
                [(c.name, c.transform, c.num_buckets) for c in partition_cls.columns]
                if partition_cls
                else None
            ),
            data_path=data_path,
            tags=list(tags.items()) if tags else None,
            if_exists=if_exists,
        )
        return TransactionTable._from_pytransaction_table(
            pytransaction_table, self._storage_options
        )

    def commit(self) -> None:
        self._pytx.commit()

    def __enter__(self) -> Transaction:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.commit()


# ----------------------------------------------------------------------------------------------- #
#                                              TABLE                                              #
# ----------------------------------------------------------------------------------------------- #


class TransactionTable:
    _pytxtable: PyTransactionTable
    _storage_options: StorageOptionSet

    @classmethod
    def _from_pytransaction_table(
        cls, pytransaction_table: PyTransactionTable, storage_options: StorageOptionSet
    ) -> TransactionTable:
        table = cls.__new__(cls)
        table._pytxtable = pytransaction_table
        table._storage_options = storage_options
        return table

    @property
    def schema(self) -> Schema:
        """The schema of the table."""
        return Schema(self._pytxtable.columns)

    @property
    def partitioning(self) -> Partitioning | None:
        """The partitioning of the table, if any."""
        partitioning = self._pytxtable.partitioning
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

    # ------------------------------------------------------------------------------------------- #
    #                                         PROPERTIES                                          #
    # ------------------------------------------------------------------------------------------- #

    @property
    def name(self) -> TableName:
        """The fully qualified name of the table."""
        return TableName(*self._pytxtable.name)

    # ------------------------------------------------------------------------------------------- #
    #                                            WRITES                                           #
    # ------------------------------------------------------------------------------------------- #

    def write_data_files(self, files: str | WriteDataFile | Sequence[str | WriteDataFile]) -> None:
        """Register existing Parquet files with the table without rewriting them.

        Args:
            files: The data files to add. This may be a single file or a sequence of files. Each
                entry may either be a path to a Parquet file or a :class:`WriteDataFile` for full
                control over statistics and partition values. Paths may be relative to the table's
                data path or absolute (including remote URLs such as ``s3://``).

        Note:
            For any file whose statistics are not provided, the statistics are computed by reading
            the file's Parquet footer while registering the file. Ownership of the added files is
            transferred to DuckLake, i.e. they may be deleted by subsequent maintenance operations
            and must not be modified externally.
        """
        if isinstance(files, (str, WriteDataFile)):
            files = [files]
        self._pytxtable.write_data_files(
            [WriteDataFile(f) if isinstance(f, str) else f for f in files]
        )

    def _get_write_info(self) -> tuple[TableMetadata, PyDataFilePathGenerator]:
        return self._pytxtable.get_write_info()

    def _write_inline_data(self, data: ArrowStreamExportable) -> None:
        self._pytxtable.write_inline_data(data)

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
        lf: pl.LazyFrame,
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
        self._pytxtable.rename(new_name)

    def update_partitioning(self, partitioning: Partitioning | None) -> None:
        """Update the partitioning of this table.

        Args:
            partitioning: The new partitioning. If `None` is provided, the partitioning of the
                table is reset.

        Note:
            This is a metadata-only operation which does not rewrite data files. As a result,
            queries might not be fully optimized.
        """
        self._pytxtable.update_partitioning(
            None
            if partitioning is None
            else [(col.name, col.transform, col.num_buckets) for col in partitioning.columns]
        )

    def add_column(self, column: Column) -> None:
        """Add a new column to the table.

        Args:
            column: The column to add.
        """
        self._pytxtable.add_column(column)

    def rename_column(self, column: str | Sequence[str], new_name: str) -> None:
        """Rename a column in the table.

        Args:
            column: The current name of the column to rename. This may be provided as a "path"
                to a nested column.
            new_name: The new name for the column.
        """
        self._pytxtable.rename_column(
            column if isinstance(column, str) else list(column),
            new_name,
        )

    def remove_column(self, column: str | Sequence[str]) -> None:
        """Remove a column from the table.

        Args:
            column: The name of the column to remove. This may be provided as a "path"
                to a nested column.
        """
        self._pytxtable.remove_column(column if isinstance(column, str) else list(column))

    def update_column_dtype(self, column: str, new_dtype: DataType) -> None:
        """Update the data type of the provided column.

        Generally speaking, data types can only be changed via type promotion. For example,
        integers can be turned into larger integers.

        For struct columns, updating the data type allows adding and dropping fields.

        Args:
            column: The column for which to change the data type.
            new_dtype: The new data type of the column.
        """
        self._pytxtable.update_column_dtype(column, new_dtype)

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
        self._pytxtable.update_column_default(column, default_value)

    def update_column_nullability(self, column: str, nullable: bool) -> None:
        """Update the nullability of a column.

        Args:
            column: The column for which to change the nullability.
            nullable: Whether the column should allow null values.
        """
        self._pytxtable.update_column_nullability(column, nullable)

    def update_schema(self, schema: Schema) -> None:
        """Update the full schema of the table.

        This is a convenience function that allows to easily add and remove multiple columns as
        well as changing the data type of existing columns.

        Args:
            schema: The new schema of the table.
        """
        self._pytxtable.update_schema(schema.columns)

    def delete(self) -> None:
        """Delete the table from the catalog.

        After calling this method, the `TransactionTable` object is no longer valid.
        """
        self._pytxtable.delete()

    def add_tag(self, key: str, value: str) -> None:
        """Add a new tag to the table.

        Args:
            key: The key of the tag.
            value: The value of the tag.
        """
        self._pytxtable.add_tag(key, value)

    def remove_tag(self, key: str) -> None:
        """Remove an existing tag from the table.

        Args:
            key: The key of the tag.

        Raises:
            ValueError: If no tag for the provided key exists.
        """
        self._pytxtable.remove_tag(key)

    def add_column_tag(self, column: str | Sequence[str], key: str, value: str) -> None:
        """Add a new tag to a column.

        Args:
            column: The name of the column to add the tag to. This may be provided as a "path"
                to a nested column.
            key: The key of the tag.
            value: The value of the tag.
        """
        self._pytxtable.add_column_tag(
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
        self._pytxtable.remove_column_tag(
            column if isinstance(column, str) else list(column),
            key,
        )
