from __future__ import annotations

import copy
import json
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeAlias, cast

from ._native import assign_new_schema_field_ids, schema_from_arrow
from .typedefs import ArrowSchemaExportable, Column, List, Map, Schema, Struct

if TYPE_CHECKING:
    from collections.abc import Mapping

    import polars as pl
    import polars.datatypes as pld

POLARS_LOGICAL_TYPES_TAG = "ducklake-sdk.polars.logical-types.v1"

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True)
class _PolarsLogicalType:
    type_name: str
    version: int
    metadata: dict[str, JsonValue]


@dataclass(frozen=True)
class _EncodedPolarsSchema:
    columns: list[Column]


class _PolarsLogicalTypeCodec(Protocol):
    """Translate one logical dtype between Polars and DuckLake representations.

    Codecs receive expressions whose dtype is the codec's logical type when encoding and its
    physical type when validating or decoding. Logical types nested in List or Struct currently
    rely on Polars' recursive composite casts.
    """

    type_name: str
    version: int

    def matches(self, dtype: pl.DataType | pld.DataTypeClass) -> bool: ...

    def physical_dtype(self, dtype: pl.DataType | pld.DataTypeClass) -> pl.DataType: ...

    def metadata(self, dtype: pl.DataType | pld.DataTypeClass) -> dict[str, JsonValue]: ...

    def logical_dtype(
        self,
        physical_dtype: pl.DataType | pld.DataTypeClass,
        metadata: Mapping[str, JsonValue],
    ) -> pl.DataType: ...

    def encode_expression(
        self,
        expression: pl.Expr,
        logical_dtype: pl.DataType | pld.DataTypeClass,
        physical_dtype: pl.DataType | pld.DataTypeClass,
    ) -> pl.Expr: ...

    def validate_physical_expression(
        self,
        expression: pl.Expr,
        logical_dtype: pl.DataType | pld.DataTypeClass,
        physical_dtype: pl.DataType | pld.DataTypeClass,
    ) -> pl.Expr: ...

    def decode_expression(
        self,
        expression: pl.Expr,
        logical_dtype: pl.DataType | pld.DataTypeClass,
        physical_dtype: pl.DataType | pld.DataTypeClass,
    ) -> pl.Expr: ...


class _EnumCodec:
    type_name = "enum"
    version = 1

    def matches(self, dtype: pl.DataType | pld.DataTypeClass) -> bool:
        import polars as pl

        return isinstance(dtype, pl.Enum)

    def physical_dtype(self, dtype: pl.DataType | pld.DataTypeClass) -> pl.DataType:
        import polars as pl

        return pl.String()

    def metadata(self, dtype: pl.DataType | pld.DataTypeClass) -> dict[str, JsonValue]:
        import polars as pl

        enum = cast(pl.Enum, dtype)
        return {"categories": enum.categories.to_list()}

    def logical_dtype(
        self,
        physical_dtype: pl.DataType | pld.DataTypeClass,
        metadata: Mapping[str, JsonValue],
    ) -> pl.DataType:
        import polars as pl

        categories = metadata.get("categories")
        if (
            physical_dtype != pl.String
            or set(metadata) != {"categories"}
            or not _are_valid_categories(categories)
        ):
            raise ValueError("Invalid Polars Enum logical type metadata")
        return pl.Enum(cast(list[str], categories))

    def encode_expression(
        self,
        expression: pl.Expr,
        logical_dtype: pl.DataType | pld.DataTypeClass,
        physical_dtype: pl.DataType | pld.DataTypeClass,
    ) -> pl.Expr:
        return expression.cast(physical_dtype)

    def validate_physical_expression(
        self,
        expression: pl.Expr,
        logical_dtype: pl.DataType | pld.DataTypeClass,
        physical_dtype: pl.DataType | pld.DataTypeClass,
    ) -> pl.Expr:
        return expression.cast(logical_dtype).cast(physical_dtype)

    def decode_expression(
        self,
        expression: pl.Expr,
        logical_dtype: pl.DataType | pld.DataTypeClass,
        physical_dtype: pl.DataType | pld.DataTypeClass,
    ) -> pl.Expr:
        return expression.cast(logical_dtype)


def _index_codecs(
    codecs: tuple[_PolarsLogicalTypeCodec, ...],
) -> dict[str, _PolarsLogicalTypeCodec]:
    result: dict[str, _PolarsLogicalTypeCodec] = {}
    for codec in codecs:
        if not codec.type_name or codec.type_name in result:
            raise RuntimeError(f"Duplicate Polars logical type codec {codec.type_name!r}")
        if type(codec.version) is not int or codec.version < 1:
            raise RuntimeError(
                f"Invalid version {codec.version!r} for Polars logical type "
                f"codec {codec.type_name!r}"
            )
        result[codec.type_name] = codec
    return result


_LOGICAL_TYPE_CODECS: tuple[_PolarsLogicalTypeCodec, ...] = (_EnumCodec(),)
_LOGICAL_TYPE_CODECS_BY_NAME = _index_codecs(_LOGICAL_TYPE_CODECS)


def schema_from_polars(schema: ArrowSchemaExportable) -> _EncodedPolarsSchema | None:
    if type(schema).__module__.partition(".")[0] != "polars":
        return None

    import polars as pl

    if not isinstance(schema, pl.Schema):
        return None

    logical_types: dict[tuple[str, ...], _PolarsLogicalType] = {}
    physical_schema = pl.Schema(
        [(name, _physical_dtype(dtype, (name,), logical_types)) for name, dtype in schema.items()]
    )
    columns = schema_from_arrow(cast(ArrowSchemaExportable, physical_schema))
    columns_by_path = dict(_walk_columns(columns))
    for path, logical_type in logical_types.items():
        columns_by_path[path]._polars_logical_type = logical_type
    return _EncodedPolarsSchema(columns)


def prepare_table_polars_metadata(
    schema: Schema, tags: Mapping[str, str] | None
) -> tuple[list[Column], dict[str, str]]:
    prepared_columns = copy.deepcopy(schema.columns)
    prepared_tags = dict(tags or {})
    logical_types_by_path: dict[tuple[str, ...], _PolarsLogicalType] = {}
    for path, column in _walk_columns(prepared_columns):
        if logical_type := column._polars_logical_type:
            logical_types_by_path[path] = logical_type
            column._polars_logical_type = None

    prepared_columns = assign_new_schema_field_ids(prepared_columns)
    columns_by_path = dict(_walk_columns(prepared_columns))
    logical_types: dict[str, JsonValue] = {}
    for path, logical_type in logical_types_by_path.items():
        field_id = columns_by_path[path].field_id
        if field_id is None:
            raise RuntimeError(f"Missing field ID for Polars logical type at path {path!r}")
        logical_types[str(field_id)] = _logical_type_payload(logical_type)

    for _, column in _walk_columns(prepared_columns):
        column.field_id = None

    if POLARS_LOGICAL_TYPES_TAG in prepared_tags:
        raise ValueError(
            f"Table tag {POLARS_LOGICAL_TYPES_TAG!r} is reserved for Polars logical types"
        )
    if logical_types:
        prepared_tags[POLARS_LOGICAL_TYPES_TAG] = json.dumps(
            logical_types,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    return prepared_columns, prepared_tags


def logical_polars_schema(schema: Schema, tags: Mapping[str, str]) -> pl.Schema:
    import polars as pl

    field_ids = {field_id for column in schema.columns for field_id in _field_ids(column)}
    logical_types = _table_logical_types(tags, field_ids)
    physical_schema = pl.Schema(schema)
    return pl.Schema(
        [
            (
                column.name,
                _logical_dtype(column, physical_schema[column.name], logical_types),
            )
            for column in schema.columns
        ]
    )


def physicalize_polars_schema(schema: pl.Schema) -> pl.Schema:
    import polars as pl

    return pl.Schema([(name, _physical_dtype(dtype)) for name, dtype in schema.items()])


def encode_polars_expression(
    expression: pl.Expr,
    logical_dtype: pl.DataType | pld.DataTypeClass,
    physical_dtype: pl.DataType | pld.DataTypeClass,
) -> pl.Expr:
    """Encode an input logical expression into its physical DuckLake representation."""
    codec = _codec_for_dtype(logical_dtype)
    if codec is not None:
        return codec.encode_expression(
            expression,
            logical_dtype,
            physical_dtype,
        )
    return expression.cast(physical_dtype)


def validate_polars_expression(
    expression: pl.Expr,
    logical_dtype: pl.DataType | pld.DataTypeClass,
    physical_dtype: pl.DataType | pld.DataTypeClass,
) -> pl.Expr:
    """Validate physical values against a logical type and keep them physical."""
    codec = _codec_for_dtype(logical_dtype)
    if codec is not None:
        return codec.validate_physical_expression(
            expression,
            logical_dtype,
            physical_dtype,
        )
    return expression.cast(logical_dtype).cast(physical_dtype)


def decode_polars_expression(
    expression: pl.Expr,
    logical_dtype: pl.DataType | pld.DataTypeClass,
    physical_dtype: pl.DataType | pld.DataTypeClass,
) -> pl.Expr:
    """Decode a physical DuckLake expression into its logical Polars representation."""
    codec = _codec_for_dtype(logical_dtype)
    if codec is not None:
        return codec.decode_expression(expression, logical_dtype, physical_dtype)
    return expression.cast(logical_dtype)


def _physical_dtype(
    dtype: pl.DataType | pld.DataTypeClass,
    path: tuple[str, ...] = (),
    logical_types: dict[tuple[str, ...], _PolarsLogicalType] | None = None,
) -> pl.DataType:
    import polars as pl

    if codec := _codec_for_dtype(dtype):
        if logical_types is not None:
            logical_types[path] = _PolarsLogicalType(
                codec.type_name,
                codec.version,
                codec.metadata(dtype),
            )
        dtype = codec.physical_dtype(dtype)
    elif isinstance(dtype, (pl.Array, pl.Categorical)) or dtype == pl.Object:
        raise NotImplementedError(f"Polars logical type {dtype!r} is not yet supported")

    if isinstance(dtype, pl.List):
        return pl.List(_physical_dtype(dtype.inner, (*path, "element"), logical_types))
    if isinstance(dtype, pl.Struct):
        return pl.Struct(
            [
                pl.Field(
                    field.name,
                    _physical_dtype(field.dtype, (*path, field.name), logical_types),
                )
                for field in dtype.fields
            ]
        )
    return cast("pl.DataType", dtype)


def _logical_dtype(
    column: Column,
    physical_dtype: pl.DataType | pld.DataTypeClass,
    logical_types: dict[int, _PolarsLogicalType],
) -> pl.DataType:
    import polars as pl

    if isinstance(column.data_type, List):
        list_dtype = cast(pl.List, physical_dtype)
        dtype: pl.DataType | pld.DataTypeClass = pl.List(
            _logical_dtype(column.data_type.inner, list_dtype.inner, logical_types)
        )
    elif isinstance(column.data_type, Struct):
        struct_dtype = cast(pl.Struct, physical_dtype)
        physical_fields = {field.name: field.dtype for field in struct_dtype.fields}
        dtype = pl.Struct(
            [
                pl.Field(
                    field.name,
                    _logical_dtype(field, physical_fields[field.name], logical_types),
                )
                for field in column.data_type.fields
            ]
        )
    else:
        dtype = physical_dtype

    logical_type = logical_types.get(cast(int, column.field_id))
    if logical_type is None:
        return cast("pl.DataType", dtype)
    codec = _LOGICAL_TYPE_CODECS_BY_NAME[logical_type.type_name]
    return codec.logical_dtype(dtype, logical_type.metadata)


def ensure_no_polars_logical_types(columns: list[Column], operation: str) -> None:
    if any(column._polars_logical_type for _, column in _walk_columns(columns)):
        raise NotImplementedError(
            f"{operation} with Polars logical types is not yet supported; "
            "create the logical type as part of a new table instead"
        )


def ensure_not_reserved_polars_tag(key: str) -> None:
    if key == POLARS_LOGICAL_TYPES_TAG:
        raise ValueError(f"Table tag {key!r} is reserved for Polars logical types")


def _logical_type_payload(logical_type: _PolarsLogicalType) -> dict[str, JsonValue]:
    if not _is_json_value(logical_type.metadata):
        raise TypeError(
            f"Metadata for Polars logical type {logical_type.type_name!r} "
            "must be JSON-serializable"
        )
    return {
        "type": logical_type.type_name,
        "version": logical_type.version,
        "metadata": logical_type.metadata,
    }


def _table_logical_types(
    tags: Mapping[str, str], current_field_ids: set[int]
) -> dict[int, _PolarsLogicalType]:
    value = tags.get(POLARS_LOGICAL_TYPES_TAG)
    if value is None:
        return {}
    try:
        raw_logical_types = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid Polars logical type metadata for table") from e
    if not isinstance(raw_logical_types, dict):
        raise ValueError("Invalid Polars logical type metadata for table")

    logical_types: dict[int, _PolarsLogicalType] = {}
    for raw_field_id, raw_logical_type in raw_logical_types.items():
        try:
            field_id = int(raw_field_id)
        except (TypeError, ValueError) as e:
            raise ValueError("Invalid Polars logical type metadata for table") from e
        if field_id < 1 or str(field_id) != raw_field_id:
            raise ValueError("Invalid Polars logical type metadata for table")
        if field_id not in current_field_ids:
            continue
        logical_types[field_id] = _parse_logical_type(raw_logical_type)
    return logical_types


def _parse_logical_type(raw_logical_type: object) -> _PolarsLogicalType:
    if not isinstance(raw_logical_type, dict) or set(raw_logical_type) != {
        "type",
        "version",
        "metadata",
    }:
        raise ValueError("Invalid Polars logical type metadata for table")
    raw_mapping = cast(dict[str, object], raw_logical_type)
    type_name = raw_mapping["type"]
    version = raw_mapping["version"]
    metadata = raw_mapping["metadata"]
    if (
        not isinstance(type_name, str)
        or type(version) is not int
        or version < 1
        or not isinstance(metadata, dict)
        or not _is_json_value(metadata)
    ):
        raise ValueError("Invalid Polars logical type metadata for table")
    codec = _LOGICAL_TYPE_CODECS_BY_NAME.get(type_name)
    if codec is None:
        raise ValueError(f"Unsupported Polars logical type {type_name!r}")
    if version != codec.version:
        raise ValueError(f"Unsupported version {version} for Polars logical type {type_name!r}")
    return _PolarsLogicalType(type_name, version, cast(dict[str, JsonValue], metadata))


def _codec_for_dtype(
    dtype: pl.DataType | pld.DataTypeClass,
) -> _PolarsLogicalTypeCodec | None:
    return next((codec for codec in _LOGICAL_TYPE_CODECS if codec.matches(dtype)), None)


def _is_json_value(value: object) -> bool:
    if value is None or isinstance(value, (bool, int, str)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and _is_json_value(item) for key, item in value.items())
    return False


def _field_ids(column: Column) -> list[int]:
    field_ids = [cast(int, column.field_id)]
    if isinstance(column.data_type, List):
        field_ids.extend(_field_ids(column.data_type.inner))
    elif isinstance(column.data_type, Struct):
        for field in column.data_type.fields:
            field_ids.extend(_field_ids(field))
    elif isinstance(column.data_type, Map):
        field_ids.extend(_field_ids(column.data_type.key))
        field_ids.extend(_field_ids(column.data_type.value))
    return field_ids


def _walk_columns(
    columns: list[Column],
    parent_path: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], Column]]:
    result: list[tuple[tuple[str, ...], Column]] = []
    for column in columns:
        path = (*parent_path, column.name)
        result.append((path, column))
        if isinstance(column.data_type, List):
            result.extend(_walk_columns([column.data_type.inner], path))
        elif isinstance(column.data_type, Struct):
            result.extend(_walk_columns(column.data_type.fields, path))
        elif isinstance(column.data_type, Map):
            result.extend(_walk_columns([column.data_type.key, column.data_type.value], path))
    return result


def _are_valid_categories(categories: object) -> bool:
    return (
        isinstance(categories, list)
        and all(isinstance(category, str) for category in categories)
        and len(categories) == len(set(categories))
    )
