from __future__ import annotations

import datetime as dt
import decimal
import uuid
from abc import ABC
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, NamedTuple, Protocol, TypeAlias, TypedDict, overload

import dateutil.relativedelta as rd

from ._native import schema_to_arrow

if TYPE_CHECKING:
    from collections.abc import Mapping

# ------------------------------------------ TABLE NAME ----------------------------------------- #


class TableName(NamedTuple):
    """A tuple representing the name of a table."""

    #: The schema of the table. The default schema is "main".
    schema: str
    #: The name of the table within the schema.
    name: str

    def __str__(self) -> str:
        schema = self.schema.replace('"', '""')
        name = self.name.replace('"', '""')
        return f'"{schema}"."{name}"'


# ---------------------------------------- TABLE METADATA --------------------------------------- #


class TableMetadata(TypedDict):
    """Metadata properties for a table."""

    #: Maximum amount of rows to inline in a single insert.
    data_inlining_row_limit: int
    #: The target data file size for insertion and compaction operations.
    target_file_size: int
    #: Number of bytes per row group in Parquet files.
    parquet_row_group_size_bytes: int | None
    #: Number of rows per row group in Parquet files.
    parquet_row_group_size: int
    #: Compression algorithm for Parquet files (uncompressed, snappy, gzip, zstd, brotli, lz4,
    #: lz4_raw).
    parquet_compression: str
    #: Compression level for Parquet files.
    parquet_compression_level: int
    #: Parquet format version (1 or 2).
    parquet_version: int
    #: If partitioned data should be written in a Hive-style folder structure.
    hive_file_pattern: bool
    #: Minimum fraction of data (0-1) that must be removed from a file before a rewrite is
    #: warranted.
    rewrite_delete_threshold: float
    #: Whether a table is included when compaction functions are called without a specific table
    #: argument.
    auto_compact: bool


class TableMetadataUpdate(TypedDict, total=False):
    """Metadata properties for a table that can be updated."""

    data_inlining_row_limit: int | None
    target_file_size: int | None
    parquet_row_group_size_bytes: int | None
    parquet_row_group_size: int | None
    parquet_compression: str | None
    parquet_compression_level: int | None
    parquet_version: int | None
    hive_file_pattern: bool | None
    rewrite_delete_threshold: float | None
    auto_compact: bool | None


class GlobalMetadataUpdate(TypedDict, total=False):
    """Metadata properties at the global or schema scope that can be updated."""

    require_commit_message: bool | None
    delete_older_than: str | None
    expire_older_than: str | None
    per_thread_output: bool | None


def _serialize_metadata_value(value: bool | int | float | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# ------------------------------------------- SNAPSHOT ------------------------------------------ #


class SnapshotMetadata:
    """Metadata for a DuckLake snapshot."""

    id: int
    timestamp: dt.datetime

    def __repr__(self) -> str:
        return f"SnapshotMetadata(id={self.id}, timestamp={self.timestamp.isoformat()})"


# ------------------------------------- MAINTENANCE RESULTS ------------------------------------ #


class MaintenanceResult(TypedDict):
    """Result row returned by file compaction maintenance operations."""

    table_name: TableName
    files_processed: int
    files_created: int


# -------------------------------------------- SCHEMA ------------------------------------------- #


class Schema:
    """Schema of a table."""

    columns: list[Column]

    def __init__(self, columns: Sequence[Column] | Mapping[str, DataType]) -> None:
        if isinstance(columns, Sequence):
            self.columns = list(columns)
        else:
            self.columns = [Column(name, dtype) for name, dtype in columns.items()]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return False
        return self.columns == other.columns

    def __repr__(self) -> str:
        return "\n".join([repr(col) for col in self.columns])

    def __arrow_c_schema__(self) -> object:
        return schema_to_arrow(self.columns).__arrow_c_schema__()


# -------------------------------------------- COLUMN ------------------------------------------- #


class Column:
    """A named column with a data type, nullability, and optional tags."""

    def __init__(
        self,
        name: str,
        data_type: DataType,
        *,
        nullable: bool = True,
        tags: Mapping[str, str] | None = None,
        initial_default: Value | None = None,
        default_value: Value | tuple[str, str] | None = None,
        field_id: int | None = None,
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.tags = dict(tags) if tags else {}
        self.initial_default = initial_default
        self.default_value = default_value
        self.field_id = field_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Column):
            return False
        return (
            self.name == other.name
            and self.data_type == other.data_type
            and self.nullable == other.nullable
            and self.tags == other.tags
            and self.initial_default == other.initial_default
            and self.default_value == other.default_value
            and self.field_id == other.field_id
        )

    def __repr__(self) -> str:
        options = []
        if not self.nullable:
            options += ["not null"]
        if self.tags:
            options += [f"tags={self.tags}"]
        if self.initial_default is not None:
            options += [f"initial_default={self.initial_default!r}"]
        if self.default_value is not None:
            options += [f"default_value={self.default_value!r}"]
        options_str = "" if not options else f" [{', '.join(options)}]"

        field_id_str = ""
        if self.field_id is not None:
            field_id_str = f" [field_id={self.field_id}]"
        return f"'{self.name}': {repr(self.data_type)}{options_str}{field_id_str}"


# ------------------------------------------ DATA TYPES ----------------------------------------- #

TimestampPrecision = Literal["seconds", "milliseconds", "microseconds", "nanoseconds"]


class DataType(ABC):
    """Base class for all supported data types."""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return True

    def __repr__(self) -> str:
        return self.__class__.__name__.lower()


class Boolean(DataType):
    """Boolean data type representing true/false values."""


class Int8(DataType):
    """8-bit signed integer data type."""


class Int16(DataType):
    """16-bit signed integer data type."""


class Int32(DataType):
    """32-bit signed integer data type."""


class Int64(DataType):
    """64-bit signed integer data type."""


class Int128(DataType):
    """128-bit signed integer data type."""


class UInt8(DataType):
    """8-bit unsigned integer data type."""


class UInt16(DataType):
    """16-bit unsigned integer data type."""


class UInt32(DataType):
    """32-bit unsigned integer data type."""


class UInt64(DataType):
    """64-bit unsigned integer data type."""


class UInt128(DataType):
    """128-bit unsigned integer data type."""


class Float32(DataType):
    """32-bit floating point data type."""


class Float64(DataType):
    """64-bit floating point data type."""


class Decimal(DataType):
    """Fixed-precision decimal data type."""

    precision: int
    scale: int

    def __init__(self, precision: int, scale: int) -> None:
        self.precision = precision
        self.scale = scale

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Decimal):
            return False
        return self.precision == other.precision and self.scale == other.scale

    def __repr__(self) -> str:
        return f"decimal({self.precision}, {self.scale})"


class Time(DataType):
    """Time of day data type without timezone."""


class TimeTz(DataType):
    """Time of day data type with timezone."""


class Date(DataType):
    """Calendar date data type."""


class Timestamp(DataType):
    """Timestamp data type without timezone."""

    def __init__(self, precision: TimestampPrecision = "microseconds") -> None:
        self.precision = precision

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Timestamp):
            return False
        return self.precision == other.precision

    def __repr__(self) -> str:
        return f"timestamp({self.precision})"


class TimestampTz(DataType):
    """Timestamp data type with timezone."""


class Interval(DataType):
    """Time interval data type."""


class Varchar(DataType):
    """Variable-length character string data type."""


class Blob(DataType):
    """Binary large object data type."""


class Json(DataType):
    """JSON data type."""


class Uuid(DataType):
    """Universally unique identifier data type."""


class List(DataType):
    """List/array data type containing elements of a single type."""

    def __init__(self, inner: DataType | Column) -> None:
        if isinstance(inner, Column):
            if inner.name != "element":
                raise ValueError("List inner column must be named 'element'")
            self.inner = inner
        else:
            self.inner = Column("element", inner)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, List):
            return False
        return self.inner == other.inner

    def __repr__(self) -> str:
        return f"list({repr(self.inner)})"


class Struct(DataType):
    """Structured data type containing named fields."""

    def __init__(self, fields: Sequence[Column] | Mapping[str, DataType]) -> None:
        self.fields = (
            list(fields)
            if isinstance(fields, Sequence)
            else [Column(name, dtype) for name, dtype in fields.items()]
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Struct):
            return False
        return self.fields == other.fields

    def __repr__(self) -> str:
        field_reprs = ", ".join([repr(field) for field in self.fields])
        return f"struct({field_reprs})"


class Map(DataType):
    """Map/dictionary data type with key-value pairs."""

    def __init__(self, key: DataType | Column, value: DataType | Column) -> None:
        if isinstance(key, Column):
            if key.name != "key":
                raise ValueError("Map key column must be named 'key'")
            self.key = key
        else:
            self.key = Column("key", key)
        if isinstance(value, Column):
            if value.name != "value":
                raise ValueError("Map value column must be named 'value'")
            self.value = value
        else:
            self.value = Column("value", value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Map):
            return False
        return self.key == other.key and self.value == other.value

    def __repr__(self) -> str:
        return f"map({repr(self.key)} => {repr(self.value)})"


# ------------------------------------------ PARTITION ------------------------------------------ #


_ParamLessPartitionTransform = Literal[None, "year", "month", "day", "hour"]
PartitionTransform = _ParamLessPartitionTransform | Literal["bucket"]


class Partitioning:
    """Partition specification for a table."""

    def __init__(
        self, columns: Sequence[PartitionColumn] | Sequence[str] | PartitionColumn | str
    ) -> None:
        if isinstance(columns, str):
            self.columns = [PartitionColumn(columns)]
        elif isinstance(columns, PartitionColumn):
            self.columns = [columns]
        else:
            if not columns:
                raise ValueError("Partition must have at least one column")
            self.columns = [
                col if isinstance(col, PartitionColumn) else PartitionColumn(col)
                for col in columns
            ]

    def __repr__(self) -> str:
        return ", ".join([repr(col) for col in self.columns])


class PartitionColumn:
    """Individual column of a (multi-column) partition."""

    @overload
    def __init__(
        self,
        name: str,
        *,
        transform: _ParamLessPartitionTransform = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        name: str,
        *,
        transform: Literal["bucket"],
        num_buckets: int,
    ) -> None: ...

    def __init__(
        self, name: str, *, transform: PartitionTransform = None, num_buckets: int | None = None
    ) -> None:
        if transform == "bucket" and (num_buckets is None or num_buckets < 1):
            raise ValueError("Bucket transform requires `num_buckets` to be a positive integer.")

        self.name = name
        self.transform = transform
        self.num_buckets = num_buckets

    def __repr__(self) -> str:
        if self.transform is not None:
            if self.transform == "bucket":
                return f"{self.name}[{self.transform}({self.num_buckets})]"
            return f"{self.name}[{self.transform}]"
        return self.name


# -------------------------------------------- VALUE -------------------------------------------- #

Value: TypeAlias = (
    bool
    | int
    | float
    | str
    | bytes
    | decimal.Decimal
    | uuid.UUID
    | dt.date
    | dt.time
    | dt.datetime
    | dt.timedelta
    | rd.relativedelta
    | list["Value"]
    | dict[str, "Value"]
    | list[tuple["Value", "Value"]]  # this is the `Map` type
)

# ----------------------------------------- DATA FILES ----------------------------------------- #


class WriteDataFile:
    """A new data file to be registered with a table."""

    def __init__(
        self,
        path: str,
        *,
        statistics: DataFileStatistics | None = None,
        partition_values: Mapping[str, Value] | None = None,
    ) -> None:
        self.path = path
        self.statistics = statistics
        self.partition_values = dict(partition_values) if partition_values else {}

    def __repr__(self) -> str:
        return (
            f"WriteDataFile("
            f"path={self.path!r}, "
            f"statistics={self.statistics!r}, "
            f"partition_values={self.partition_values})"
        )


class DataFileStatistics:
    """Statistics for a data file."""

    def __init__(
        self,
        num_rows: int,
        *,
        file_size_bytes: int | None = None,
        footer_size_bytes: int | None = None,
        column_stats: Mapping[int, ColumnStats] | None = None,
    ) -> None:
        self.num_rows = num_rows
        self.file_size_bytes = file_size_bytes
        self.footer_size_bytes = footer_size_bytes
        self.column_stats = dict(column_stats) if column_stats else {}

    def __repr__(self) -> str:
        return (
            f"DataFileStatistics("
            f"num_rows={self.num_rows}, "
            f"file_size_bytes={self.file_size_bytes}, "
            f"footer_size_bytes={self.footer_size_bytes}, "
            f"column_stats={self.column_stats})"
        )


class ColumnStats:
    """Statistics for a single column in a data file."""

    def __init__(
        self,
        *,
        size_bytes: int | None = None,
        min_value: Value | None = None,
        max_value: Value | None = None,
        null_count: int | None = None,
        contains_nan: bool | None = None,
    ) -> None:
        self.size_bytes = size_bytes
        self.min_value = min_value
        self.max_value = max_value
        self.null_count = null_count
        self.contains_nan = contains_nan

    def __repr__(self) -> str:
        return (
            f"ColumnStats(size_bytes={self.size_bytes}, "
            f"min_value={self.min_value!r}, "
            f"max_value={self.max_value!r}, "
            f"null_count={self.null_count}, "
            f"contains_nan={self.contains_nan})"
        )


class DeleteFile:
    """A delete file to be registered with a table."""

    def __init__(
        self,
        path: str,
        num_deletes: int,
        *,
        file_size_bytes: int | None = None,
        footer_size_bytes: int | None = None,
    ) -> None:
        self.path = path
        self.num_deletes = num_deletes
        self.file_size_bytes = file_size_bytes
        self.footer_size_bytes = footer_size_bytes

    def __repr__(self) -> str:
        return (
            f"DeleteFile(path={self.path!r}, "
            f"num_deletes={self.num_deletes}, "
            f"file_size_bytes={self.file_size_bytes}, "
            f"footer_size_bytes={self.footer_size_bytes})"
        )


class ScanDataFile:
    """A data file with its associated delete files from a scan."""

    def __init__(
        self,
        path: str,
        statistics: DataFileStatistics,
        delete_files: list[DeleteFile],
        inline_deletes: ArrowArrayExportable | None,
    ) -> None:
        self.path = path
        self.statistics = statistics
        self.delete_files = delete_files
        self.inline_deletes = inline_deletes

    def __repr__(self) -> str:
        return (
            f"ScanDataFile(path={self.path!r}, "
            f"statistics={self.statistics!r}, "
            f"delete_files={self.delete_files!r}, "
            f"inline_deletes={self.inline_deletes!r})"
        )


class ScanResult:
    """Result of scanning a table, containing all data files and their delete files."""

    data_files: list[ScanDataFile]
    inline_data: list[ArrowArrayExportable]

    def __init__(
        self,
        data_files: list[ScanDataFile],
        inline_data: list[ArrowArrayExportable],
    ) -> None:
        self.data_files = data_files
        self.inline_data = inline_data

    def __repr__(self) -> str:
        return f"ScanResult(data_files={self.data_files!r}, inline_data={self.inline_data!r})"


# -------------------------------------------- ARROW -------------------------------------------- #


class ArrowSchemaExportable(Protocol):
    """Type protocol for Arrow C Schema Interface via Arrow PyCapsule Interface."""

    def __arrow_c_schema__(self) -> object: ...


class ArrowArrayExportable(ArrowSchemaExportable, Protocol):
    """Type protocol for Arrow C Data Interface via Arrow PyCapsule Interface."""

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]: ...


class ArrowStreamExportable(Protocol):
    """Type protocol for Arrow C Data Interface via Arrow PyCapsule Interface."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
