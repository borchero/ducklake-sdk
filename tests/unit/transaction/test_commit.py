from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest
import sqlalchemy as sa

import ducklake as dl

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture()
def catalog_engine(catalog_url: str) -> Iterator[sa.Engine]:
    engine = sa.create_engine(catalog_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.mark.parametrize("rename", [False, True])
def test_alter_pending_columns(ducklake: dl.Ducklake, catalog_url: str, rename: bool) -> None:
    # Arrange
    name = "renamed" if rename else "table"

    # Act
    with ducklake.transaction() as tx:
        table = tx.create_table("table", {"x": dl.Int64()})
        table.add_column(dl.Column("y", dl.Int64()))
        table.rename_column("y", "z")
        if rename:
            table.rename(name)

    # Assert
    with dl.connect(catalog_url) as reopened:
        assert reopened.table(name).schema.columns == [
            dl.Column("x", dl.Int64(), field_id=1),
            dl.Column("z", dl.Int64(), field_id=2),
        ]


def test_edit_pending_tags(ducklake: dl.Ducklake, catalog_url: str) -> None:
    # Arrange
    tags = {"env": "old"}

    # Act
    with ducklake.transaction() as tx:
        table = tx.create_table("table", {"x": dl.Int64()}, tags=tags)
        table.add_tag("env", "new")
        table.add_tag("temporary", "value")
        table.remove_tag("temporary")

    # Assert
    with dl.connect(catalog_url) as reopened:
        assert reopened.table("table").tags == {"env": "new"}


@pytest.fixture(params=[False, True], ids=["new-statistics", "existing-statistics"])
def statistics_tables(
    ducklake: dl.Ducklake, request: pytest.FixtureRequest
) -> tuple[list[str], int]:
    names = ["first", "second"]
    with ducklake.transaction() as tx:
        for name in names:
            tx.create_table(name, {"x": dl.Int64(), "y": dl.Int64()})
    if request.param:
        _write_files(ducklake, names, [0])
    return names, int(request.param)


def test_table_statistics_across_multiple_writes(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, statistics_tables: tuple[list[str], int]
) -> None:
    # Arrange
    names, seeded_files = statistics_tables
    files_per_table = seeded_files + 2

    # Act
    _write_files(ducklake, names, [1, 2])

    # Assert
    with catalog_engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats ORDER BY table_id"
            )
        ).all()
    assert rows == [
        (
            files_per_table * (10 + index),
            files_per_table * (10 + index),
            files_per_table * (100 + index),
        )
        for index in range(2)
    ]


def test_column_statistics_across_multiple_writes(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, statistics_tables: tuple[list[str], int]
) -> None:
    # Arrange
    names, _ = statistics_tables

    # Act
    _write_files(ducklake, names, [1, 2])

    # Assert
    with catalog_engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT min_value, max_value, contains_null FROM ducklake_table_column_stats "
                "ORDER BY table_id, column_id"
            )
        ).all()
    assert rows == [
        ("-3", "3", True),
        ("-4", "4", False),
        ("-13", "13", True),
        ("-14", "14", False),
    ]


@pytest.mark.parametrize("seeded", [False, True])
@pytest.mark.parametrize("column_tag", [False, True], ids=["table-tag", "column-tag"])
def test_tag_retirement_uses_database_collation(
    ducklake: dl.Ducklake,
    catalog_engine: sa.Engine,
    catalog_url: str,
    seeded: bool,
    column_tag: bool,
) -> None:
    # Arrange
    with catalog_engine.connect() as connection:
        keys_equal = connection.scalar(sa.select(sa.literal("Owner") == sa.literal("owner")))
    columns = [dl.Column("x", dl.Int64(), tags={"Owner": "old"} if column_tag else {})]
    tags = {} if column_tag else {"Owner": "old"}
    if seeded:
        ducklake.create_table("table", columns, tags=tags)

    # Act
    with ducklake.transaction() as tx:
        table = tx.table("table") if seeded else tx.create_table("table", columns, tags=tags)
        if column_tag:
            table.add_column_tag("x", "owner", "new")
        else:
            table.add_tag("owner", "new")

    # Assert
    with dl.connect(catalog_url) as reopened:
        table = reopened.table("table")
        actual = table.schema.columns[0].tags if column_tag else table.tags
    assert actual == ({"owner": "new"} if keys_equal else {"Owner": "old", "owner": "new"})


@pytest.fixture(params=[False, True], ids=["new-table", "existing-table"])
def existing_table(ducklake: dl.Ducklake, request: pytest.FixtureRequest) -> bool:
    if request.param:
        ducklake.create_table("table", {"x": dl.Int64()})
    return request.param


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
@pytest.mark.parametrize("inline_first", [False, True], ids=["file-first", "inline-first"])
def test_mixed_writes_allocate_row_ids_in_request_order(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, existing_table: bool, inline_first: bool
) -> None:
    # Arrange
    inline_data = pl.DataFrame({"x": [1, 2]})
    file = dl.WriteDataFile(
        "file.parquet",
        statistics=dl.DataFileStatistics(num_rows=3),
    )

    # Act
    with ducklake.transaction() as tx:
        table = (
            tx.table("table") if existing_table else tx.create_table("table", {"x": dl.Int64()})
        )
        if inline_first:
            table.write_polars(inline_data)
        table.write_data_files([file])
        if not inline_first:
            table.write_polars(inline_data)

    # Assert
    with catalog_engine.connect() as connection:
        file_start = connection.scalar(sa.text("SELECT row_id_start FROM ducklake_data_file"))
        inline_name = connection.scalar(
            sa.text("SELECT table_name FROM ducklake_inlined_data_tables")
        )
        inline_table = sa.table(inline_name, sa.column("row_id"))
        inline_ids = (
            connection.execute(sa.select(inline_table.c.row_id).order_by(inline_table.c.row_id))
            .scalars()
            .all()
        )
    assert file_start == (2 if inline_first else 0)
    assert inline_ids == ([0, 1] if inline_first else [3, 4])


def test_removed_pending_column_reserves_field_id(
    ducklake: dl.Ducklake, catalog_url: str, existing_table: bool
) -> None:
    # Arrange
    next_column = dl.Column("next", dl.Int64())

    # Act
    with ducklake.transaction() as tx:
        table = (
            tx.table("table") if existing_table else tx.create_table("table", {"x": dl.Int64()})
        )
        table.add_column(dl.Column("temporary", dl.Int64()))
        table.remove_column("temporary")
    with dl.connect(catalog_url) as reopened:
        table = reopened.table("table")
        table.add_column(next_column)
        columns = table.schema.columns

    # Assert
    assert columns == [
        dl.Column("x", dl.Int64(), field_id=1),
        dl.Column("next", dl.Int64(), field_id=3),
    ]


@pytest.fixture()
def tables_with_history(ducklake: dl.Ducklake) -> list[str]:
    names = ["first", "second"]
    with ducklake.transaction() as tx:
        for name in names:
            tx.create_table(name, {"x": dl.Int64()})
    _rename_tables(ducklake, names, "_previous")
    return [f"{name}_previous" for name in names]


def test_renames_preserve_table_metadata(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, tables_with_history: list[str]
) -> None:
    # Arrange
    metadata = sa.text(
        "SELECT table_id, table_uuid, schema_id, path, path_is_relative FROM ducklake_table "
        "WHERE end_snapshot IS NULL ORDER BY table_id"
    )
    with catalog_engine.connect() as connection:
        before = connection.execute(metadata).all()

    # Act
    _rename_tables(ducklake, tables_with_history, "_renamed")

    # Assert
    with catalog_engine.connect() as connection:
        assert connection.execute(metadata).all() == before


def test_renames_preserve_table_history(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, tables_with_history: list[str]
) -> None:
    # Arrange
    names = tables_with_history

    # Act
    _rename_tables(ducklake, names, "_renamed")

    # Assert
    with catalog_engine.connect() as connection:
        history = (
            connection.execute(
                sa.text("SELECT table_name FROM ducklake_table ORDER BY table_id, begin_snapshot")
            )
            .scalars()
            .all()
        )
    assert history == [
        "first",
        "first_previous",
        "first_previous_renamed",
        "second",
        "second_previous",
        "second_previous_renamed",
    ]


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_inline_write_uses_latest_schema_registration(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine
) -> None:
    # Arrange
    with ducklake.transaction() as tx:
        tx.create_table("table", {"x": dl.Int64()})
    with ducklake.transaction() as tx:
        tx.table("table").add_column(dl.Column("y", dl.Int64()))
    registrations = sa.table(
        "ducklake_inlined_data_tables",
        sa.column("table_id"),
        sa.column("table_name"),
        sa.column("schema_version"),
    )
    with catalog_engine.begin() as connection:
        rows = (
            connection.execute(
                sa.select(registrations).order_by(registrations.c.schema_version.desc())
            )
            .mappings()
            .all()
        )
        latest_name = rows[0]["table_name"]
        connection.execute(sa.delete(registrations))
        connection.execute(sa.insert(registrations), [dict(row) for row in rows])

    # Act
    with ducklake.transaction() as tx:
        tx.table("table").write_polars(pl.DataFrame({"x": [1], "y": [2]}))

    # Assert
    latest = sa.table(latest_name, sa.column("x"), sa.column("y"))
    with catalog_engine.connect() as connection:
        assert connection.execute(sa.select(latest)).all() == [(1, 2)]


@pytest.fixture()
def column_stats_update_log(ducklake: dl.Ducklake, catalog_engine: sa.Engine) -> None:
    table = ducklake.create_table("table", {"x": dl.Float64(), "y": dl.Float64()})
    statistics = dl.DataFileStatistics(
        num_rows=10,
        column_stats={
            column_id: dl.ColumnStats(
                min_value=0.0, max_value=10.0, null_count=0, contains_nan=False
            )
            for column_id in [1, 2]
        },
    )
    table.write_data_files([dl.WriteDataFile("seed.parquet", statistics=statistics)])
    with catalog_engine.begin() as connection:
        connection.execute(sa.text("CREATE TABLE column_stats_updates (column_id BIGINT)"))
        connection.execute(
            sa.text(
                "CREATE TRIGGER log_column_stats_update AFTER UPDATE ON ducklake_table_column_stats "
                "FOR EACH ROW BEGIN INSERT INTO column_stats_updates VALUES (NEW.column_id); END"
            )
        )


@pytest.mark.usefixtures("column_stats_update_log")
@pytest.mark.skip_config(
    catalog="postgres", reason="Backend-independent filtering tested on SQLite."
)
@pytest.mark.skip_config(catalog="mysql", reason="Backend-independent filtering tested on SQLite.")
def test_only_changed_column_statistics_are_updated(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine
) -> None:
    # Arrange
    statistics = dl.DataFileStatistics(
        num_rows=10,
        column_stats={
            1: dl.ColumnStats(min_value=-1.0, max_value=9.0, null_count=0, contains_nan=False),
            2: dl.ColumnStats(min_value=1.0, max_value=9.0, null_count=0, contains_nan=False),
        },
    )

    # Act
    with ducklake.transaction() as tx:
        for name in ["first.parquet", "second.parquet"]:
            tx.table("table").write_data_files([dl.WriteDataFile(name, statistics=statistics)])

    # Assert
    with catalog_engine.connect() as connection:
        updates = (
            connection.execute(sa.text("SELECT column_id FROM column_stats_updates"))
            .scalars()
            .all()
        )
    assert updates == [1]


@pytest.fixture()
def file_metadata_tables(ducklake: dl.Ducklake) -> list[str]:
    names = ["first", "second"]
    with ducklake.transaction() as tx:
        for name in names:
            tx.create_table(name, {"x": dl.Int64(), "y": dl.Int64()})
    return names


def test_file_column_statistics_across_multiple_writes(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, file_metadata_tables: list[str]
) -> None:
    # Arrange
    names = file_metadata_tables

    # Act
    _write_files(ducklake, names, [1, 2])

    # Assert
    with catalog_engine.connect() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT t.table_name, d.path, f.column_id, f.min_value, f.max_value "
                "FROM ducklake_file_column_stats f JOIN ducklake_data_file d "
                "ON f.data_file_id = d.data_file_id AND f.table_id = d.table_id "
                "JOIN ducklake_table t ON f.table_id = t.table_id "
                "ORDER BY t.table_name, d.path, f.column_id"
            )
        ).all()
    assert rows == [
        (
            name,
            f"file_{write}.parquet",
            column_id,
            str(-(2 if write == 1 else 1) - table_index * 10 - column_id),
            str(write + table_index * 10 + column_id),
        )
        for table_index, name in enumerate(names)
        for write in [1, 2]
        for column_id in [1, 2]
    ]


# -------------------------------------------- UTILS -------------------------------------------- #


def _write_files(ducklake: dl.Ducklake, names: list[str], writes: list[int]) -> None:
    with ducklake.transaction() as tx:
        for write in writes:
            for table_index, name in enumerate(names):
                statistics = dl.DataFileStatistics(
                    num_rows=10 + table_index,
                    file_size_bytes=100 + table_index,
                    column_stats={
                        column_id: dl.ColumnStats(
                            min_value=-(2 if write == 1 else 1) - table_index * 10 - column_id,
                            max_value=write + table_index * 10 + column_id,
                            null_count=int(write == 1 and column_id == 1),
                        )
                        for column_id in [1, 2]
                    },
                )
                tx.table(name).write_data_files(
                    [dl.WriteDataFile(f"file_{write}.parquet", statistics=statistics)]
                )


def _rename_tables(ducklake: dl.Ducklake, names: list[str], suffix: str) -> None:
    with ducklake.transaction() as tx:
        for name in names:
            tx.table(name).rename(f"{name}{suffix}")
