from collections.abc import Callable
from contextlib import nullcontext
from datetime import date
from typing import Literal

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import ducklake as dl
import ducklake.exceptions as dlexc


@pytest.mark.parametrize(("macro_type", "sql"), [("scalar", "a + b"), ("table", "SELECT a + b")])
def test_macro_lifecycle(
    shared_ducklake: dl.Ducklake,
    random_macro_name: str,
    macro_type: dl.MacroType,
    sql: str,
) -> None:
    # Arrange
    parameters = [
        dl.MacroParameter("a", data_type="int64"),
        dl.MacroParameter("b", default_value="10", default_value_type="int32"),
    ]

    # Act
    created = shared_ducklake.create_macro(
        random_macro_name, sql, parameters, macro_type=macro_type
    )
    fetched = shared_ducklake.get_macro(random_macro_name, macro_type=macro_type)
    name, kind, implementations = created.name, created.macro_type, fetched.implementations
    shared_ducklake.delete_macro(random_macro_name, macro_type=macro_type)

    # Assert
    assert name == ("main", random_macro_name)
    assert kind == macro_type
    assert implementations == [dl.MacroImplementation(sql, parameters)]
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.get_macro(random_macro_name, macro_type=macro_type)


def test_create_overloaded_macro(shared_ducklake: dl.Ducklake, random_macro_name: str) -> None:
    # Arrange
    connection = shared_ducklake._duckdb_connection
    implementations = [
        dl.MacroImplementation("x + 1", [dl.MacroParameter("x", data_type="int32")]),
        dl.MacroImplementation("x || '!'", [dl.MacroParameter("x", data_type="varchar")]),
    ]

    # Act
    macro = shared_ducklake.create_macro(random_macro_name, implementations)
    result = connection.execute(
        f"SELECT {random_macro_name}(41::INTEGER), {random_macro_name}('hi'::VARCHAR)"
    ).fetchone()

    # Assert
    assert macro.implementations == implementations
    assert result == (42, "hi!")


def test_list_macros_filters_schema(
    shared_ducklake: dl.Ducklake,
    macro_schema: str,
    random_macro_name: str,
) -> None:
    # Arrange
    macro = shared_ducklake.create_macro((macro_schema, random_macro_name), "x + 1", ["x"])

    # Act
    macros = shared_ducklake.list_macros(macro_schema)

    # Assert
    assert [candidate.name for candidate in macros] == [macro.name]


def test_create_macro_if_exists_skip(shared_ducklake: dl.Ducklake, random_macro_name: str) -> None:
    # Arrange
    shared_ducklake.create_macro(random_macro_name, "x + 1", ["x"])

    # Act
    macro = shared_ducklake.create_macro(random_macro_name, "x + 2", ["x"], if_exists="skip")

    # Assert
    assert macro.implementations[0].sql == "x + 1"


@pytest.mark.parametrize("if_not_exists", ["fail", "skip"])
@pytest.mark.parametrize("missing_schema", [False, True])
def test_delete_missing_macro(
    shared_ducklake: dl.Ducklake,
    random_macro_name: str,
    random_schema_name: str,
    if_not_exists: Literal["fail", "skip"],
    missing_schema: bool,
) -> None:
    # Arrange
    name = (random_schema_name if missing_schema else "main", random_macro_name)
    snapshot_id = shared_ducklake.get_latest_snapshot().id

    # Act
    with pytest.raises(dlexc.NotFoundError) if if_not_exists == "fail" else nullcontext():
        shared_ducklake.delete_macro(name, if_not_exists=if_not_exists)

    # Assert
    assert shared_ducklake.get_latest_snapshot().id == snapshot_id


def test_delete_schema_cascade_deletes_macro(
    shared_ducklake: dl.Ducklake,
    macro_schema: str,
    random_macro_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_macro((macro_schema, random_macro_name), "42")
    shared_ducklake.create_macro(
        (macro_schema, random_macro_name),
        "SELECT 42 AS value",
        macro_type="table",
    )

    # Act
    shared_ducklake.delete_schema(macro_schema, cascade=True)

    # Assert
    assert macro_schema not in shared_ducklake.list_schemas()


def test_scalar_and_table_macros_with_same_name_are_independent(
    shared_ducklake: dl.Ducklake, random_macro_name: str
) -> None:
    # Arrange
    shared_ducklake.create_macro(random_macro_name, "41 + 1")
    shared_ducklake.create_macro(
        random_macro_name,
        "SELECT 42 AS value",
        macro_type="table",
    )
    connection = shared_ducklake._duckdb_connection

    # Act
    matching = [
        macro for macro in shared_ducklake.list_macros() if macro.name.name == random_macro_name
    ]
    macro_types = {macro.macro_type for macro in matching}
    shared_ducklake.delete_macro(random_macro_name)
    table_result = connection.execute(f"SELECT * FROM {random_macro_name}()").fetchone()

    # Assert
    assert macro_types == {"scalar", "table"}
    assert shared_ducklake.get_macro(random_macro_name, macro_type="table").macro_type == "table"
    with pytest.raises(dlexc.NotFoundError):
        shared_ducklake.get_macro(random_macro_name)
    assert table_result == (42,)


def test_sdk_loads_duckdb_scalar_and_table_macros_with_same_name(
    shared_ducklake: dl.Ducklake, random_macro_name: str
) -> None:
    # Arrange
    shared_ducklake.execute_sql(f"CREATE MACRO {random_macro_name}() AS 42")
    shared_ducklake.execute_sql(f"CREATE MACRO {random_macro_name}() AS TABLE SELECT 42 AS value")

    # Act
    matching = [
        macro for macro in shared_ducklake.list_macros() if macro.name.name == random_macro_name
    ]

    # Assert
    assert {macro.macro_type for macro in matching} == {"scalar", "table"}


def test_macro_time_travel(ducklake: dl.Ducklake, random_macro_name: str) -> None:
    # Arrange
    ducklake.create_macro(random_macro_name, "42")
    snapshot_id = ducklake.get_latest_snapshot().id
    ducklake.delete_macro(random_macro_name)

    # Act
    historical = ducklake.at(snapshot_id).get_macro(random_macro_name)
    current_macros = ducklake.list_macros()

    # Assert
    assert historical.implementations == [dl.MacroImplementation("42")]
    assert current_macros == []


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        (
            "(x BIGINT) AS x + 1",
            dl.MacroImplementation("(x + 1)", [dl.MacroParameter("x", data_type="int64")]),
        ),
        (
            "(x := [1, 2]) AS x",
            dl.MacroImplementation(
                "x", [dl.MacroParameter("x", default_value="[1, 2]", default_value_type="list")]
            ),
        ),
    ],
)
def test_sdk_can_read_duckdb_macro(
    ducklake: dl.Ducklake,
    random_macro_name: str,
    definition: str,
    expected: dl.MacroImplementation,
) -> None:
    # Arrange
    ducklake.execute_sql(f"CREATE MACRO {random_macro_name}{definition}")

    # Act
    macro = ducklake.get_macro(random_macro_name)

    # Assert
    assert macro.macro_type == "scalar"
    assert macro.implementations == [expected]


def test_polars_view_expands_scalar_macro(
    shared_ducklake: dl.Ducklake,
    macro_table: dl.Table,
    random_view_name: str,
    random_macro_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_macro(
        random_macro_name,
        "value * factor",
        [
            "value",
            dl.MacroParameter("factor", default_value="2", default_value_type="int32"),
        ],
    )
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT {random_macro_name}(x) AS doubled, "
        f"{random_macro_name}(x, factor := 3) AS tripled FROM {macro_table.name}",
    )

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, pl.DataFrame({"doubled": [2, 4, 6], "tripled": [3, 6, 9]}))


def test_polars_view_expands_table_macro(
    shared_ducklake: dl.Ducklake,
    macro_table: dl.Table,
    random_view_name: str,
    random_macro_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_macro(
        random_macro_name,
        f"SELECT x FROM {macro_table.name} WHERE x > threshold",
        ["threshold"],
        macro_type="table",
    )
    view = shared_ducklake.create_view(
        random_view_name, f"SELECT {random_macro_name}.x FROM {random_macro_name}(1)"
    )

    # Act
    actual = view.read_polars()

    # Assert
    assert_frame_equal(actual, pl.DataFrame({"x": [2, 3]}))


def test_polars_macro_resolution_is_case_insensitive_and_falls_back_to_main(
    shared_ducklake: dl.Ducklake,
    macro_schema: str,
    random_view_name: str,
    random_macro_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_macro(random_macro_name.upper(), "x + 1", ["X"])
    view = shared_ducklake.create_view(
        (macro_schema, random_view_name),
        f"SELECT {random_macro_name.lower()}(1) AS value",
    )

    # Act
    actual = view.read_polars()

    # Assert
    assert actual.item() == 2


@pytest.mark.parametrize(
    ("value", "data_type", "expected"),
    [
        ("hello", "varchar", "hello"),
        ("NULL", "varchar", "NULL"),
        ("true", "boolean", True),
        ("1", "boolean", True),
        ("0", "boolean", False),
        ("2024-01-02", "date", date(2024, 1, 2)),
        ("42", "int32", 42),
    ],
)
def test_macro_defaults_match_duckdb_and_polars(
    shared_ducklake: dl.Ducklake,
    create_default_macro: Callable[[str, str], dl.Macro],
    random_macro_name: str,
    random_view_name: str,
    value: str,
    data_type: str,
    expected: object,
) -> None:
    # Arrange
    create_default_macro(value, data_type)
    view = shared_ducklake.create_view(random_view_name, f"SELECT {random_macro_name}()")

    # Act
    duckdb_result = shared_ducklake._duckdb_connection.execute(
        f"SELECT * FROM {view.name}"
    ).fetchone()
    polars_result = view.read_polars().item()

    # Assert
    assert duckdb_result == (expected,)
    assert polars_result == expected


def test_macro_blob_defaults(
    shared_ducklake: dl.Ducklake,
    create_default_macro: Callable[[str, str], dl.Macro],
    random_macro_name: str,
) -> None:
    # Arrange
    create_default_macro("a\\xAAb", "blob")

    # Act
    result = shared_ducklake._duckdb_connection.execute(f"SELECT {random_macro_name}()").fetchone()

    # Assert
    assert result == (b"a\xaab",)


@pytest.mark.parametrize(
    ("body", "parameters", "call", "expected"),
    [
        ("x * 2", ["x"], "(x + 1)", 8),
        ("x + 1", ["x"], "(x) * 2", 8),
        ("x + y", ["x", "y"], "(y + 1, x + 2)", 11),
    ],
)
def test_polars_macro_preserves_argument_scope_and_precedence(
    shared_ducklake: dl.Ducklake,
    random_macro_name: str,
    random_view_name: str,
    body: str,
    parameters: list[str],
    call: str,
    expected: int,
) -> None:
    # Arrange
    shared_ducklake.create_macro(random_macro_name, body, parameters)
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT {random_macro_name}{call} AS result FROM (SELECT 3 AS x, 5 AS y)",
    )

    # Act
    result = view.read_polars().item()

    # Assert
    assert result == expected


def test_polars_rejects_typed_macro_parameters(
    shared_ducklake: dl.Ducklake,
    random_view_name: str,
    random_macro_name: str,
) -> None:
    # Arrange
    shared_ducklake.create_macro(
        random_macro_name,
        "value",
        [dl.MacroParameter("value", data_type="int32")],
    )
    view = shared_ducklake.create_view(random_view_name, f"SELECT {random_macro_name}(42)")

    # Act
    with pytest.raises(ValueError, match="typed parameters") as exc_info:
        view.read_polars()

    # Assert
    assert random_macro_name in str(exc_info.value)


def test_polars_view_expands_nested_macros(
    shared_ducklake: dl.Ducklake,
    random_view_name: str,
    random_macro_name: str,
) -> None:
    # Arrange
    wrapper_name = f"{random_macro_name}_wrapper"
    shared_ducklake.create_macro(random_macro_name, "value + 1", ["value"])
    shared_ducklake.create_macro(
        wrapper_name,
        f"{random_macro_name}(value) * 2",
        ["value"],
    )
    view = shared_ducklake.create_view(
        random_view_name,
        f"SELECT {wrapper_name}({random_macro_name}(3))",
    )

    # Act
    result = view.read_polars().item()

    # Assert
    assert result == 10


@pytest.mark.parametrize(
    ("macro_type", "sql"),
    [("scalar", "SELECT 1"), ("scalar", "1 WHERE false"), ("table", "1 + 1")],
)
def test_create_macro_rejects_invalid_definition(
    shared_ducklake: dl.Ducklake,
    random_macro_name: str,
    macro_type: dl.MacroType,
    sql: str,
) -> None:
    # Act
    with pytest.raises(ValueError) as exc_info:
        shared_ducklake.create_macro(random_macro_name, sql, macro_type=macro_type)

    # Assert
    assert "macro" in str(exc_info.value)


@pytest.mark.parametrize(
    "parameter",
    [
        dl.MacroParameter("value", data_type="not_a_type"),
        dl.MacroParameter("value", default_value="oops", default_value_type="int32"),
        dl.MacroParameter("value", default_value="NULL", default_value_type="int32"),
        dl.MacroParameter("value", default_value="[1, 2]", default_value_type="list"),
        dl.MacroParameter(
            "value",
            data_type="int32",
            default_value="hello",
            default_value_type="varchar",
        ),
    ],
)
def test_create_macro_rejects_invalid_parameter_metadata(
    shared_ducklake: dl.Ducklake,
    random_macro_name: str,
    parameter: dl.MacroParameter,
) -> None:
    # Act
    with pytest.raises(ValueError) as exc_info:
        shared_ducklake.create_macro(random_macro_name, "value", [parameter])

    # Assert
    assert "parameter" in str(exc_info.value)


def test_create_macro_rejects_case_insensitive_duplicate_parameters(
    shared_ducklake: dl.Ducklake, random_macro_name: str
) -> None:
    # Act
    with pytest.raises(ValueError) as exc_info:
        shared_ducklake.create_macro(random_macro_name, "x", ["x", "X"])

    # Assert
    assert "duplicate" in str(exc_info.value)


def test_create_macro_rejects_overloads_duplicated_after_type_normalization(
    shared_ducklake: dl.Ducklake, random_macro_name: str
) -> None:
    # Arrange
    implementations = [
        dl.MacroImplementation("value", [dl.MacroParameter("value", data_type="decimal(10,2)")]),
        dl.MacroImplementation("value", [dl.MacroParameter("value", data_type="DECIMAL(10, 2)")]),
    ]

    # Act
    with pytest.raises(ValueError) as exc_info:
        shared_ducklake.create_macro(random_macro_name, implementations)

    # Assert
    assert "unique parameter type signatures" in str(exc_info.value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"default_value": "hello"},
        {"default_value_type": "varchar"},
    ],
)
def test_macro_parameter_requires_default_value_and_type_together(
    kwargs: dict[str, str],
) -> None:
    # Act
    with pytest.raises(ValueError) as exc_info:
        dl.MacroParameter("value", **kwargs)

    # Assert
    assert "default_value" in str(exc_info.value)
