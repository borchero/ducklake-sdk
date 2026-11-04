from collections.abc import Callable

import polars as pl
import pytest

import ducklake as dl


@pytest.fixture()
def macro_schema(shared_ducklake: dl.Ducklake, random_schema_name: str) -> str:
    shared_ducklake.create_schema(random_schema_name)
    return random_schema_name


@pytest.fixture()
def create_default_macro(
    shared_ducklake: dl.Ducklake, random_macro_name: str
) -> Callable[[str, str], dl.Macro]:
    def create(value: str, data_type: str) -> dl.Macro:
        return shared_ducklake.create_macro(
            random_macro_name,
            "value",
            [dl.MacroParameter("value", default_value=value, default_value_type=data_type)],
        )

    return create


@pytest.fixture()
def macro_table(shared_ducklake: dl.Ducklake, random_table_name: str) -> dl.Table:
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    table.sink_polars(pl.LazyFrame({"x": [1, 2, 3]}))
    return table
