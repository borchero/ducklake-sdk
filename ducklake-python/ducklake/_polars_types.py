from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from ._native import schema_from_arrow
from .typedefs import ArrowSchemaExportable, Column, List, Map, Schema, Struct

if TYPE_CHECKING:
    from collections.abc import Mapping

    import polars as pl
    import polars.datatypes as pld

POLARS_LOGICAL_TYPES_TAG = "ducklake-sdk.polars.logical-types.v1"


@dataclass(frozen=True)
class _PolarsLogicalType:
    type_name: str
    metadata: dict[str, object]


@dataclass(frozen=True)
class _EncodedPolarsSchema:
    columns: list[Column]
    logical_types: dict[tuple[str, ...], _PolarsLogicalType]


class _PolarsLogicalTypeCodec(Protocol):
    type_name: str

    def matches(self, dtype: pl.DataType | pld.DataTypeClass) -> bool: ...

    def physical_dtype(self, dtype: pl.DataType | pld.DataTypeClass) -> pl.DataType: ...

    def metadata(self, dtype: pl.DataType | pld.DataTypeClass) -> dict[str, object]: ...

    def logical_dtype(
        self,
        physical_dtype: pl.DataType | pld.DataTypeClass,
        metadata: Mapping[str, object],
    ) -> pl.DataType: ...


class _EnumCodec:
    type_name = "enum"

    def matches(self, dtype: pl.DataType | pld.DataTypeClass) -> bool:
        import polars as pl

        return isinstance(dtype, pl.Enum)

    def physical_dtype(self, dtype: pl.DataType | pld.DataTypeClass) -> pl.DataType:
        import polars as pl

        return pl.String()

    def metadata(self, dtype: pl.DataType | pld.DataTypeClass) -> dict[str, object]:
        import polars as pl

        enum = cast(pl.Enum, dtype)
        return {"categories": enum.categories.to_list()}

    def logical_dtype(
        self,
        physical_dtype: pl.DataType | pld.DataTypeClass,
        metadata: Mapping[str, object],
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


_LOGICAL_TYPE_CODECS: tuple[_PolarsLogicalTypeCodec, ...] = (_EnumCodec(),)
_LOGICAL_TYPE_CODECS_BY_NAME = {codec.type_name: codec for codec in _LOGICAL_TYPE_CODECS}


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
    return _EncodedPolarsSchema(columns, logical_types)


def prepare_table_polars_metadata(
    schema: Schema, tags: Mapping[str, str] | None
) -> tuple[list[Column], dict[str, str]]:
    prepared_columns = copy.deepcopy(schema.columns)
    prepared_tags: dict[str, str] = dict(tags or {})
    logical_types: dict[str, dict[str, object]] = {}
    unmatched_paths = set(schema._polars_logical_types)
    next_field_id = 1

    def visit(column: Column, path: tuple[str, ...]) -> None:
        nonlocal next_field_id

        field_id = next_field_id
        next_field_id += 1
        if logical_type := schema._polars_logical_types.get(path):
            unmatched_paths.remove(path)
            logical_types[str(field_id)] = {
                "type": logical_type.type_name,
                **logical_type.metadata,
            }

        if isinstance(column.data_type, List):
            visit(column.data_type.inner, (*path, "element"))
        elif isinstance(column.data_type, Struct):
            for field in column.data_type.fields:
                visit(field, (*path, field.name))
        elif isinstance(column.data_type, Map):
            visit(column.data_type.key, (*path, "key"))
            visit(column.data_type.value, (*path, "value"))

    for column in prepared_columns:
        visit(column, (column.name,))

    if unmatched_paths:
        paths = ", ".join(".".join(path) for path in sorted(unmatched_paths))
        raise ValueError(f"Polars logical type metadata references unknown fields: {paths}")
    if logical_types:
        if POLARS_LOGICAL_TYPES_TAG in prepared_tags:
            raise ValueError(
                f"Table tag {POLARS_LOGICAL_TYPES_TAG!r} is reserved for Polars logical types"
            )
        prepared_tags[POLARS_LOGICAL_TYPES_TAG] = json.dumps(
            logical_types, ensure_ascii=False, separators=(",", ":")
        )
    return prepared_columns, prepared_tags


def logical_polars_schema(schema: Schema, tags: Mapping[str, str]) -> pl.Schema:
    import polars as pl

    logical_types = _table_logical_types(tags)
    physical_schema = pl.Schema(schema)
    logical_schema = pl.Schema(
        [
            (
                column.name,
                _logical_dtype(column, physical_schema[column.name], logical_types),
            )
            for column in schema.columns
        ]
    )
    field_ids = {field_id for column in schema.columns for field_id in _field_ids(column)}
    if unknown_field_ids := logical_types.keys() - field_ids:
        unknown = ", ".join(map(str, sorted(unknown_field_ids)))
        raise ValueError(f"Polars logical type metadata references unknown field IDs: {unknown}")
    return logical_schema


def physicalize_polars_schema(schema: pl.Schema) -> pl.Schema:
    import polars as pl

    return pl.Schema([(name, _physical_dtype(dtype)) for name, dtype in schema.items()])


def _physical_dtype(
    dtype: pl.DataType | pld.DataTypeClass,
    path: tuple[str, ...] = (),
    logical_types: dict[tuple[str, ...], _PolarsLogicalType] | None = None,
) -> pl.DataType:
    import polars as pl

    if codec := _codec_for_dtype(dtype):
        if logical_types is not None:
            logical_types[path] = _PolarsLogicalType(codec.type_name, codec.metadata(dtype))
        dtype = codec.physical_dtype(dtype)

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


def _table_logical_types(tags: Mapping[str, str]) -> dict[int, _PolarsLogicalType]:
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
        if (
            field_id < 1
            or str(field_id) != raw_field_id
            or not isinstance(raw_logical_type, dict)
            or not isinstance(type_name := raw_logical_type.get("type"), str)
        ):
            raise ValueError("Invalid Polars logical type metadata for table")
        if type_name not in _LOGICAL_TYPE_CODECS_BY_NAME:
            raise ValueError(f"Unsupported Polars logical type {type_name!r}")
        logical_types[field_id] = _PolarsLogicalType(
            type_name,
            {key: value for key, value in raw_logical_type.items() if key != "type"},
        )
    return logical_types


def _codec_for_dtype(
    dtype: pl.DataType | pld.DataTypeClass,
) -> _PolarsLogicalTypeCodec | None:
    return next((codec for codec in _LOGICAL_TYPE_CODECS if codec.matches(dtype)), None)


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


def _are_valid_categories(categories: object) -> bool:
    return (
        isinstance(categories, list)
        and all(isinstance(category, str) for category in categories)
        and len(categories) == len(set(categories))
    )
