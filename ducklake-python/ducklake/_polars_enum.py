from __future__ import annotations

import copy
import json
from typing import TYPE_CHECKING, cast

from ._native import schema_from_arrow
from .typedefs import ArrowSchemaExportable, Column, List, Schema, Struct, Varchar

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import polars as pl
    import polars.datatypes as pld

POLARS_ENUM_TAG = "ducklake-sdk.polars.enums.v1"


def columns_from_polars_schema(schema: ArrowSchemaExportable) -> list[Column] | None:
    if type(schema).__module__.partition(".")[0] != "polars":
        return None

    import polars as pl

    if not isinstance(schema, pl.Schema):
        return None

    physical_schema = pl.Schema([(name, _physical_dtype(dtype)) for name, dtype in schema.items()])
    columns = schema_from_arrow(cast(ArrowSchemaExportable, physical_schema))
    for column, (_, dtype) in zip(columns, schema.items(), strict=True):
        _attach_enum_tags(column, dtype)
    return columns


def enum_categories(column: Column) -> list[str] | None:
    value = column.tags.get(POLARS_ENUM_TAG)
    if value is None:
        return None
    if not isinstance(column.data_type, Varchar):
        raise ValueError(
            f"Polars Enum metadata on column {column.name!r} requires a varchar data type"
        )

    try:
        categories = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid Polars Enum metadata on column {column.name!r}") from e
    if not _are_valid_categories(categories):
        raise ValueError(f"Invalid Polars Enum metadata on column {column.name!r}")
    return cast(list[str], categories)


def prepare_table_polars_metadata(
    columns: Sequence[Column], tags: Mapping[str, str] | None
) -> tuple[list[Column], dict[str, str]]:
    prepared_columns = copy.deepcopy(list(columns))
    prepared_tags: dict[str, str] = dict(tags or {})
    enum_definitions: dict[str, list[str]] = {}
    next_field_id = 1

    def visit(column: Column) -> None:
        nonlocal next_field_id

        field_id = next_field_id
        next_field_id += 1
        categories = enum_categories(column)
        column.tags.pop(POLARS_ENUM_TAG, None)
        if categories is not None:
            enum_definitions[str(field_id)] = categories

        if isinstance(column.data_type, List):
            visit(column.data_type.inner)
        elif isinstance(column.data_type, Struct):
            for field in column.data_type.fields:
                visit(field)

    for column in prepared_columns:
        visit(column)

    if enum_definitions:
        if POLARS_ENUM_TAG in prepared_tags:
            raise ValueError(f"Table tag {POLARS_ENUM_TAG!r} is reserved for Polars Enum metadata")
        prepared_tags[POLARS_ENUM_TAG] = json.dumps(
            enum_definitions, ensure_ascii=False, separators=(",", ":")
        )
    return prepared_columns, prepared_tags


def logical_polars_schema(schema: Schema, tags: Mapping[str, str]) -> pl.Schema:
    import polars as pl

    enum_definitions = _table_enum_definitions(tags)
    physical_schema = pl.Schema(schema)
    logical_schema = pl.Schema(
        [
            (
                column.name,
                _logical_dtype(column, physical_schema[column.name], enum_definitions),
            )
            for column in schema.columns
        ]
    )
    field_ids = {field_id for column in schema.columns for field_id in _field_ids(column)}
    if unknown_field_ids := enum_definitions.keys() - field_ids:
        unknown = ", ".join(map(str, sorted(unknown_field_ids)))
        raise ValueError(f"Polars Enum metadata references unknown field IDs: {unknown}")
    return logical_schema


def physicalize_polars_schema(schema: pl.Schema) -> pl.Schema:
    import polars as pl

    return pl.Schema([(name, _physical_dtype(dtype)) for name, dtype in schema.items()])


def _physical_dtype(dtype: pl.DataType | pld.DataTypeClass) -> pl.DataType:
    import polars as pl

    if isinstance(dtype, pl.Enum):
        return pl.String()
    if isinstance(dtype, pl.List):
        return pl.List(_physical_dtype(dtype.inner))
    if isinstance(dtype, pl.Struct):
        return pl.Struct(
            [pl.Field(field.name, _physical_dtype(field.dtype)) for field in dtype.fields]
        )
    return cast("pl.DataType", dtype)


def _logical_dtype(
    column: Column,
    physical_dtype: pl.DataType | pld.DataTypeClass,
    enum_definitions: dict[int, list[str]],
) -> pl.DataType:
    import polars as pl

    categories = enum_definitions.get(cast(int, column.field_id))
    if categories is not None:
        if not isinstance(column.data_type, Varchar):
            raise ValueError(
                f"Polars Enum metadata on column {column.name!r} requires a varchar data type"
            )
        return pl.Enum(categories)
    if isinstance(column.data_type, List):
        list_dtype = cast(pl.List, physical_dtype)
        return pl.List(_logical_dtype(column.data_type.inner, list_dtype.inner, enum_definitions))
    if isinstance(column.data_type, Struct):
        struct_dtype = cast(pl.Struct, physical_dtype)
        physical_fields = {field.name: field.dtype for field in struct_dtype.fields}
        return pl.Struct(
            [
                pl.Field(
                    field.name,
                    _logical_dtype(field, physical_fields[field.name], enum_definitions),
                )
                for field in column.data_type.fields
            ]
        )
    return cast("pl.DataType", physical_dtype)


def _table_enum_definitions(tags: Mapping[str, str]) -> dict[int, list[str]]:
    value = tags.get(POLARS_ENUM_TAG)
    if value is None:
        return {}
    try:
        raw_definitions = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid Polars Enum metadata for table") from e
    if not isinstance(raw_definitions, dict):
        raise ValueError("Invalid Polars Enum metadata for table")

    definitions: dict[int, list[str]] = {}
    for raw_field_id, categories in raw_definitions.items():
        try:
            field_id = int(raw_field_id)
        except (TypeError, ValueError) as e:
            raise ValueError("Invalid Polars Enum metadata for table") from e
        if field_id < 1 or str(field_id) != raw_field_id or not _are_valid_categories(categories):
            raise ValueError("Invalid Polars Enum metadata for table")
        definitions[field_id] = cast(list[str], categories)
    return definitions


def _field_ids(column: Column) -> list[int]:
    field_ids = [cast(int, column.field_id)]
    if isinstance(column.data_type, List):
        field_ids.extend(_field_ids(column.data_type.inner))
    elif isinstance(column.data_type, Struct):
        for field in column.data_type.fields:
            field_ids.extend(_field_ids(field))
    return field_ids


def _are_valid_categories(categories: object) -> bool:
    return (
        isinstance(categories, list)
        and all(isinstance(category, str) for category in categories)
        and len(categories) == len(set(categories))
    )


def _attach_enum_tags(column: Column, dtype: pl.DataType | pld.DataTypeClass) -> None:
    import polars as pl

    if isinstance(dtype, pl.Enum):
        if not isinstance(column.data_type, Varchar):
            raise ValueError(f"Polars Enum column {column.name!r} must use a varchar data type")
        column.tags[POLARS_ENUM_TAG] = json.dumps(
            dtype.categories.to_list(), ensure_ascii=False, separators=(",", ":")
        )
    elif isinstance(dtype, pl.List):
        list_dtype = cast(List, column.data_type)
        _attach_enum_tags(list_dtype.inner, dtype.inner)
    elif isinstance(dtype, pl.Struct):
        struct_dtype = cast(Struct, column.data_type)
        for field, polars_field in zip(struct_dtype.fields, dtype.fields, strict=True):
            _attach_enum_tags(field, polars_field.dtype)
