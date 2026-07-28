from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

from ._native import schema_from_arrow
from .typedefs import ArrowSchemaExportable, Column, List, Struct, Varchar

if TYPE_CHECKING:
    import polars as pl
    import polars.datatypes as pld

POLARS_ENUM_TAG = "ducklake-sdk.polars.enum.v1"


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
    if (
        not isinstance(categories, list)
        or any(not isinstance(category, str) for category in categories)
        or len(categories) != len(set(categories))
    ):
        raise ValueError(f"Invalid Polars Enum metadata on column {column.name!r}")
    return cast(list[str], categories)


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
