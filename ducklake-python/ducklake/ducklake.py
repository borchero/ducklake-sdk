from __future__ import annotations

import sys
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, overload

from .table import Table
from .transaction import Transaction
from .typedefs import (
    Column,
    DataType,
    GlobalMetadataUpdate,
    MaintenanceResult,
    PartitionColumn,
    Partitioning,
    Schema,
    SnapshotMetadata,
    TableMetadataUpdate,
    TableName,
    _serialize_metadata_value,
)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Mapping, Sequence

    import duckdb
    import sqlalchemy as sa

    from ._native import PyDucklake
    from ._storage import StorageOptionSet
    from .connect import ConnectionArgs


class Ducklake:
    """A connection to a DuckLake instance."""

    _pyducklake: PyDucklake
    _connection_args: ConnectionArgs
    _storage_options: StorageOptionSet

    @classmethod
    def _from_pyducklake(
        cls,
        pyducklake: PyDucklake,
        connection_args: ConnectionArgs,
        storage_options: StorageOptionSet,
    ) -> Ducklake:
        ducklake = cls.__new__(cls)
        ducklake._pyducklake = pyducklake
        ducklake._connection_args = connection_args
        ducklake._storage_options = storage_options
        return ducklake

    @cached_property
    def _duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        return _make_duckdb_connection(
            self._connection_args, storage_options=self._storage_options
        )

    # ---------------------------------------- SNAPSHOTS ---------------------------------------- #

    def at(self, at: int | dt.datetime, /) -> Ducklake:
        """Time travel to a specific snapshot in the catalog.

        Args:
            at: The ID of the snapshot to time travel to, or a timestamp to find the latest
                snapshot before that timestamp.

        Returns:
            A new :class:`Ducklake` instance time traveled to the specified snapshot.
        """
        pyducklake = (
            self._pyducklake.at_snapshot_id(at)
            if isinstance(at, int)
            else self._pyducklake.at_snapshot_timestamp(at)
        )
        return Ducklake._from_pyducklake(pyducklake, self._connection_args, self._storage_options)

    def get_latest_snapshot(self) -> SnapshotMetadata:
        """Get metadata for the latest snapshot in the catalog.

        Returns:
            Metadata for the latest snapshot in the catalog.
        """
        return self._pyducklake.get_latest_snapshot()

    def list_snapshots(self) -> list[SnapshotMetadata]:
        """List metadata for all snapshots in the catalog.

        When time-traveling, this returns only the snapshot that was travelled to.

        Returns:
            A list of metadata for all snapshots in the catalog, ordered from newest to oldest.
        """
        return self._pyducklake.list_snapshots()

    # ------------------------------------------- SQL ------------------------------------------- #

    def execute_sql(self, query: str | sa.ReturnsRows) -> None:
        """Execute an arbitrary SQL query against the catalog database.

        Args:
            query: The SQL query to execute. This may either be a raw string or a `sqlalchemy`
                query. If a raw string is provided, it must use the DuckDB SQL dialect. If a
                `sqlalchemy` query is provided, :mod:`duckdb-engine` must be installed.

        Note:
            This requires :mod:`duckdb` to be installed.
        """
        if isinstance(query, str):
            query_str = query
            query_params = None
        else:
            import duckdb_engine

            compiler = query.compile(dialect=duckdb_engine.Dialect(paramstyle="qmark"))
            query_str = compiler.string
            query_params = list(compiler.params.values())

            print(query_str, query_params)

        self._duckdb_connection.execute(query_str, query_params)

    # ------------------------------------------------------------------------------------------- #
    #                                          OPERATIONS                                         #
    # ------------------------------------------------------------------------------------------- #

    def transaction(
        self,
        *,
        author: str | None = None,
        message: str | None = None,
        extra_info: str | None = None,
    ) -> Transaction:
        """Start a new transaction against the catalog.

        Args:
            author: Optional author attached to the snapshot created on commit.
            message: Optional commit message attached to the snapshot.
            extra_info: Optional additional structured info attached to the snapshot.

        Returns:
            A new :class:`Transaction`. If used as a context manager, the transaction is
            automatically committed on successful exit.
        """
        pytransaction = self._pyducklake.transaction(author, message, extra_info)
        return Transaction._from_pytransaction(pytransaction, self._storage_options)

    # ----------------------------------------- SCHEMAS ----------------------------------------- #

    def create_schema(
        self,
        name: str,
        *,
        data_path: str | None = None,
        if_exists: Literal["fail", "skip"] = "fail",
    ) -> None:
        """Create a new schema in the catalog.

        Args:
            name: The name of the new schema.
            data_path: Optional data path for the schema. If not provided, it defaults to the
                schema name.
            if_exists: The strategy to apply if a schema with the same name already exists.
                "fail" raises an :class:`~ducklake.exceptions.AlreadyExistsError`, while "skip"
                leaves the existing schema unchanged.
        """
        self._pyducklake.create_schema(name, data_path=data_path, if_exists=if_exists)

    def list_schemas(self) -> list[str]:
        """List all schema names in the catalog.

        Returns:
            A list of all schema names in the catalog.
        """
        return self._pyducklake.list_schemas()

    def delete_schema(self, name: str) -> None:
        self._pyducklake.delete_schema(name)

    # ------------------------------------------ TABLES ----------------------------------------- #

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
    ) -> Table:
        """Create a new table in the catalog.

        Args:
            name: The fully qualified name of the new table.
            schema: The schema of the new table.
            partition_by: Optional partitioning for the table.
            data_path: Optional data path for the table.
            tags: Optional tags to attach to the table.
            if_exists: The strategy to apply if a table with the same name already exists.
                "fail" raises an :class:`~ducklake.exceptions.AlreadyExistsError`, while "skip"
                returns the existing table unchanged.

        Returns:
            The newly created :class:`Table`.
        """
        schema_cls = schema if isinstance(schema, Schema) else Schema(schema)
        partition_cls = (
            partition_by
            if isinstance(partition_by, Partitioning)
            else (Partitioning(partition_by) if partition_by is not None else None)
        )
        pytable = self._pyducklake.create_table(
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
        return Table._from_pytable(pytable, lambda: self._duckdb_connection, self._storage_options)

    def get_table(self, name: str | tuple[str, str] | TableName) -> Table:
        """Read a table from the catalog.

        Args:
            name: The name of the table. This can either be a string or a TableName tuple. If
                a string is provided, it is parsed just like DuckDB parses table names: it must
                be of the format `<schema>.<table>` where the schema is optional and defaults to
                "main". If either the schema or table name contains special characters, both must
                be quoted using double quotes.

        Returns:
            The Table object.

        Raises:
            NotFoundError: If the table does not exist.
        """
        pytable = self._pyducklake.table(name)
        return Table._from_pytable(pytable, lambda: self._duckdb_connection, self._storage_options)

    def list_tables(self, schema: str | None = None) -> list[Table]:
        """List all tables in the catalog.

        Args:
            schema: Optional schema name to filter tables by. If None, returns all tables
                across all schemas.

        Returns:
            A list of all Table objects in the catalog, optionally filtered by schema.
        """
        pytables = self._pyducklake.list_tables(schema)
        return [
            Table._from_pytable(pytable, lambda: self._duckdb_connection, self._storage_options)
            for pytable in pytables
        ]

    # ----------------------------------------- METADATA ---------------------------------------- #

    @overload
    def set_metadata(  # ty: ignore[invalid-overload]
        self, *, schema: str | None = None, **options: Unpack[TableMetadataUpdate]
    ) -> None: ...

    @overload
    def set_metadata(  # ty: ignore[invalid-overload]
        self, **options: Unpack[GlobalMetadataUpdate]
    ) -> None: ...

    def set_metadata(
        self,
        *,
        schema: str | None = None,
        **options: bool | int | float | str | None,
    ) -> None:
        """Set one or more metadata options at the global or schema scope.

        Provide options as keyword arguments. Pass `None` as a value to remove the option
        from the metadata (i.e. revert it to its default).

        Args:
            schema: Optional schema name to scope the table-level options to. If not provided,
                the options are set globally. Only valid for keys in
                :class:`TableMetadataUpdate`.

        Raises:
            ValueError: If a key is read-only and cannot be set.

        See also:
            :meth:`Table.set_metadata` for setting metadata options at the table scope.
        """
        for key, value in options.items():
            self._pyducklake.set_metadata(key, _serialize_metadata_value(value), schema)

    # ------------------------------------------------------------------------------------------- #
    #                                         MAINTENANCE                                         #
    # ------------------------------------------------------------------------------------------- #

    def checkpoint(self) -> None:
        """Run all recommended maintenance operations on the catalog.

        Executes the DuckDB `CHECKPOINT` statement which flushes inlined data, expires
        snapshots, merges adjacent files, rewrites files with deletes and cleans up orphaned
        files. The behavior is configured via the `rewrite_delete_threshold`,
        `delete_older_than`, `expire_older_than` and `auto_compact` metadata options.

        Note:
            This requires :mod:`duckdb` to be installed.
        """
        self._duckdb_connection.execute("CHECKPOINT;")

    # ------------------------------------- EXPIRE SNAPSHOTS ------------------------------------ #

    @overload
    def expire_snapshots(self, *, dry_run: bool = False) -> list[SnapshotMetadata]: ...

    @overload
    def expire_snapshots(
        self, *, versions: Sequence[int], dry_run: bool = False
    ) -> list[SnapshotMetadata]: ...

    @overload
    def expire_snapshots(
        self, *, older_than: dt.datetime, dry_run: bool = False
    ) -> list[SnapshotMetadata]: ...

    def expire_snapshots(
        self,
        *,
        versions: Sequence[int] | None = None,
        older_than: dt.datetime | None = None,
        dry_run: bool = False,
    ) -> list[SnapshotMetadata]:
        """Expire snapshots in the catalog so the data they reference can be cleaned up.

        This does not immediately delete the underlying files; call :meth:`cleanup_old_files`
        afterwards (or use :meth:`checkpoint`).

        The latest snapshot is always retained. If neither `versions` nor `older_than` is
        provided, snapshots are expired according to the `"expire_older_than"` metadata option.
        If that option is not set, no snapshots are expired.

        Args:
            versions: A list of snapshot IDs to expire. Versions that do not exist or refer to the
                latest snapshot are silently ignored.
            older_than: If provided, expire all snapshots created before this timestamp.
            dry_run: If `True`, no snapshots are actually expired and the returned snapshots
                merely indicate what would be expired.

        Returns:
            The snapshots that were expired, or would be expired when `dry_run` is `True`.
        """
        return self._pyducklake.expire_snapshots(
            list(versions) if versions is not None else None,
            older_than,
            dry_run,
        )

    # ------------------------------------ CLEANUP OLD FILES ------------------------------------ #

    @overload
    def cleanup_old_files(self, *, dry_run: bool = False) -> list[str]: ...

    @overload
    def cleanup_old_files(self, *, cleanup_all: bool, dry_run: bool = False) -> list[str]: ...

    @overload
    def cleanup_old_files(
        self, *, older_than: dt.datetime, dry_run: bool = False
    ) -> list[str]: ...

    def cleanup_old_files(
        self,
        *,
        cleanup_all: bool = False,
        older_than: dt.datetime | None = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Delete files that have been scheduled for deletion.

        Files are only scheduled for deletion when the snapshots referencing them are expired
        (see :meth:`expire_snapshots`).

        If neither `cleanup_all` nor `older_than` is provided, files are deleted according to the
        `"delete_older_than"` metadata option (which defaults to two days). The reason for this
        "grace period" is to prevent deleting files that are being used by active queries.

        Args:
            cleanup_all: If `True`, delete all files scheduled for deletion regardless of age.
            older_than: If provided, only delete files scheduled for deletion before this
                timestamp.
            dry_run: If `True`, no files are actually deleted and the returned paths merely
                indicate what would be deleted.

        Returns:
            The paths that were deleted, or would be deleted when `dry_run` is `True`.
        """
        return self._pyducklake.cleanup_old_files(cleanup_all, older_than, dry_run)

    # ---------------------------------- DELETE ORPHANED FILES ---------------------------------- #

    @overload
    def delete_orphaned_files(self, *, dry_run: bool = False) -> list[str]: ...

    @overload
    def delete_orphaned_files(self, *, cleanup_all: bool, dry_run: bool = False) -> list[str]: ...

    @overload
    def delete_orphaned_files(
        self, *, older_than: dt.datetime, dry_run: bool = False
    ) -> list[str]: ...

    def delete_orphaned_files(
        self,
        *,
        cleanup_all: bool = False,
        older_than: dt.datetime | None = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Delete files in the data directory that are not tracked in the catalog database.

        This is useful for cleaning up files that were written but never registered (e.g. due to a
        crashed writer).

        If neither `cleanup_all` nor `older_than` is provided, files are deleted according to the
        `"delete_older_than"` metadata option (which defaults to two days). The reason for this
        "grace period" is to prevent deleting files that are currently being written.

        Args:
            cleanup_all: If `True`, delete all orphaned files regardless of age.
            older_than: If provided, only delete orphaned files last modified before this
                timestamp.
            dry_run: If `True`, no files are actually deleted.

        Returns:
            The paths that were deleted, or would be deleted when `dry_run` is `True`.
        """
        return self._pyducklake.delete_orphaned_files(cleanup_all, older_than, dry_run)

    # ------------------------------------------ OTHER ------------------------------------------ #

    def merge_adjacent_files(
        self,
        *,
        max_compacted_files: int | None = None,
        min_file_size: int | None = None,
        max_file_size: int | None = None,
    ) -> list[MaintenanceResult]:
        """Merge small adjacent data files into larger ones across the catalog.

        Dispatches to `ducklake_merge_adjacent_files`. Only tables with `auto_compact`
        enabled are considered.

        Args:
            max_compacted_files: Maximum number of compaction operations produced in a single
                call (per table).
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
                f"CALL ducklake_merge_adjacent_files('my_ducklake'{params});",
                args,
            )
        )

    def rewrite_data_files(
        self, *, delete_threshold: float | None = None
    ) -> list[MaintenanceResult]:
        """Rewrite data files with a high fraction of deleted rows across the catalog.

        Dispatches to `ducklake_rewrite_data_files`. Files containing more deletes than
        `delete_threshold` are rewritten without the deleted rows.

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
                f"CALL ducklake_rewrite_data_files('my_ducklake'{params});",
                args,
            )
        )

    # ------------------------------------------------------------------------------------------- #
    #                                          DISCONNECT                                         #
    # ------------------------------------------------------------------------------------------- #

    def disconnect(self) -> None:
        """Disconnect from the catalog database, gracefully closing all underlying connections.

        After calling this method, all subsequent operations on this :class:`Ducklake` instance
        (or any :class:`Table` / :class:`Transaction` derived from it) will fail.

        This is normally not required because connections are released when the instance is garbage
        collected, but it is useful when you need to ensure that all connections are released
        deterministically (e.g. before dropping the catalog database).
        """
        # Close the DuckDB connection first if it has been created. The DuckDB DuckLake extension
        # keeps its own connection to the catalog database alive which would otherwise prevent
        # the catalog database from being dropped.
        if "_duckdb_connection" in self.__dict__:
            self._duckdb_connection.close()
            del self.__dict__["_duckdb_connection"]
        self._pyducklake.disconnect()

    # ------------------------------------------------------------------------------------------- #
    #                                            DUNDER                                           #
    # ------------------------------------------------------------------------------------------- #

    def __repr__(self) -> str:
        return f'Ducklake(url="{str(self._connection_args)}")'

    def __enter__(self) -> Ducklake:
        return self

    def __exit__(self, *args: Any) -> None:  # noqa: ANN401
        self.disconnect()


# -------------------------------------------- UTILS -------------------------------------------- #


def _make_duckdb_connection(
    args: ConnectionArgs,
    *,
    data_path: str | None = None,
    storage_options: StorageOptionSet | None = None,
) -> duckdb.DuckDBPyConnection:
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL ducklake;")

    # Build options based on parameters
    init_options = ""
    if data_path is not None:
        if not data_path.endswith("/"):
            data_path += "/"
        init_options += f"DATA_PATH '{data_path}'"
    if init_options:
        init_options = f"({init_options})"

    # Attach based on the URL
    match args.dialect:
        case "postgresql":
            options = []
            if args.database is not None:
                options.append(f"dbname={args.database}")
            if args.username is not None:
                options.append(f"user={args.username}")
            if args.password is not None:
                options.append(f"password={args.password}")
            if args.host is not None:
                options.append(f"host={args.host}")
            if args.port is not None:
                options.append(f"port={args.port}")

            con.execute("INSTALL postgres;")
            con.execute(
                f"ATTACH 'ducklake:postgres:{' '.join(options)}' AS my_ducklake {init_options};"
            )
        case "sqlite":
            con.execute("INSTALL sqlite;")
            con.execute(f"ATTACH 'ducklake:sqlite:{args.database}' AS my_ducklake {init_options};")
        case "mysql":
            warnings.warn(
                "Using the DuckDB DuckLake extension with MySQL is currently not recommended. See also: https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database#mysql.",
                category=UserWarning,
            )
            options = []
            if args.database is not None:
                options.append(f"database={args.database}")
            if args.username is not None:
                options.append(f"user={args.username}")
            if args.password is not None:
                options.append(f"password={args.password}")
            if args.host is not None:
                options.append(f"host={args.host}")
            if args.port is not None:
                options.append(f"port={args.port}")

            con.execute("INSTALL mysql;")
            con.execute(
                f"ATTACH 'ducklake:mysql:{' '.join(options)}' AS my_ducklake {init_options};"
            )

    # Apply storage options
    if storage_options:
        storage_options.apply_to_duckdb_connection(con)

    con.execute("USE my_ducklake;")
    return con
