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
def test_create_and_alter_in_one_commit(
    ducklake: dl.Ducklake, catalog_url: str, rename: bool
) -> None:
    # Arrange
    name = "renamed" if rename else "table"

    # Act
    with ducklake.transaction() as tx:
        table = tx.create_table("table", {"x": dl.Int64(), "y": dl.Int64()}, tags={"env": "old"})
        table.add_tag("env", "new")
        table.add_tag("temporary", "value")
        table.remove_tag("temporary")
        table.add_column(dl.Column("z", dl.Int64()))
        table.rename_column("z", "new_z")
        table.remove_column("y")
        if rename:
            table.rename(name)

    # Assert
    with dl.connect(catalog_url) as reopened:
        table = reopened.table(name)
        assert table.tags == {"env": "new"}
        assert table.schema.columns == [
            dl.Column("x", dl.Int64(), field_id=1),
            dl.Column("new_z", dl.Int64(), field_id=3),
        ]


@pytest.mark.parametrize("seeded", [False, True], ids=["new-statistics", "existing-statistics"])
@pytest.mark.parametrize(("table_count", "column_count"), [(2, 3), (3, 100)])
def test_statistics_across_multiple_writes(
    ducklake: dl.Ducklake,
    catalog_engine: sa.Engine,
    seeded: bool,
    table_count: int,
    column_count: int,
) -> None:
    # Arrange
    names = [f"table_{index}" for index in range(table_count)]
    with ducklake.transaction() as tx:
        for name in names:
            tx.create_table(name, {f"column_{index}": dl.Int64() for index in range(column_count)})
    statistics = {
        name: [
            dl.DataFileStatistics(
                num_rows=10,
                file_size_bytes=100,
                column_stats={
                    column.field_id: dl.ColumnStats(
                        min_value=-index - table_index * 1000 - column_index,
                        max_value=10 + index + table_index * 1000 + column_index,
                        null_count=index if column_index % 2 == 0 else 0,
                    )
                    for column_index, column in enumerate(ducklake.table(name).schema.columns)
                    if column.field_id is not None
                },
            )
            for index in range(3)
        ]
        for table_index, name in enumerate(names)
    }
    if seeded:
        with ducklake.transaction() as tx:
            for name in names:
                tx.table(name).write_data_files(
                    [dl.WriteDataFile("seed.parquet", statistics=statistics[name][0])]
                )

    # Act
    with ducklake.transaction() as tx:
        for index in range(1, 3):
            for name in names:
                tx.table(name).write_data_files(
                    [dl.WriteDataFile(f"file_{index}.parquet", statistics=statistics[name][index])]
                )

    # Assert
    files_per_table = 2 + seeded
    with catalog_engine.connect() as connection:
        table_stats = connection.execute(
            sa.text("SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats")
        ).all()
        column_stats = connection.execute(
            sa.text(
                "SELECT min_value, max_value, contains_null FROM ducklake_table_column_stats ORDER BY table_id, column_id"
            )
        ).all()
        row_ids = (
            connection.execute(
                sa.text(
                    "SELECT row_id_start FROM ducklake_data_file ORDER BY table_id, row_id_start"
                )
            )
            .scalars()
            .all()
        )
    assert (
        table_stats
        == [(files_per_table * 10, files_per_table * 10, files_per_table * 100)] * table_count
    )
    assert column_stats == [
        (
            str(-2 - table_index * 1000 - column_index),
            str(12 + table_index * 1000 + column_index),
            column_index % 2 == 0,
        )
        for table_index in range(table_count)
        for column_index in range(column_count)
    ]
    assert row_ids == list(range(0, files_per_table * 10, 10)) * table_count


@pytest.mark.parametrize("seeded", [False, True])
@pytest.mark.parametrize("column_tag", [False, True], ids=["table-tag", "column-tag"])
def test_tag_retirement_uses_database_collation(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, seeded: bool, column_tag: bool
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
    tag_table = sa.table(
        "ducklake_column_tag" if column_tag else "ducklake_tag",
        sa.column("key"),
        sa.column("value"),
        sa.column("end_snapshot"),
    )
    with catalog_engine.connect() as connection:
        tags = {
            key: value
            for key, value in connection.execute(
                sa.select(tag_table.c.key, tag_table.c.value).where(
                    tag_table.c.end_snapshot.is_(None)
                )
            )
        }
    assert tags == ({"owner": "new"} if keys_equal else {"Owner": "old", "owner": "new"})


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
@pytest.mark.parametrize("seeded", [False, True], ids=["new-table", "existing-table"])
@pytest.mark.parametrize("inline_first", [False, True], ids=["file-first", "inline-first"])
def test_mixed_writes_allocate_row_ids_in_request_order(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, seeded: bool, inline_first: bool
) -> None:
    # Arrange
    if seeded:
        ducklake.create_table("table", {"x": dl.Int64()})
    inline_data = pl.DataFrame({"x": [1, 2]})
    file = dl.WriteDataFile(
        "file.parquet",
        statistics=dl.DataFileStatistics(num_rows=3),
    )

    # Act
    with ducklake.transaction() as tx:
        table = tx.table("table") if seeded else tx.create_table("table", {"x": dl.Int64()})
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
        assert isinstance(inline_name, str)
        inline_table = sa.table(inline_name, sa.column("row_id"))
        inline_ids = (
            connection.execute(sa.select(inline_table.c.row_id).order_by(inline_table.c.row_id))
            .scalars()
            .all()
        )
        stats = connection.execute(
            sa.text("SELECT record_count, next_row_id FROM ducklake_table_stats")
        ).one()
    assert file_start == (2 if inline_first else 0)
    assert inline_ids == ([0, 1] if inline_first else [3, 4])
    assert stats == (5, 5)


@pytest.mark.parametrize("seeded", [False, True])
def test_removed_pending_column_reserves_field_id(
    ducklake: dl.Ducklake, catalog_url: str, seeded: bool
) -> None:
    # Arrange
    if seeded:
        ducklake.create_table("table", {"x": dl.Int64()})

    # Act
    with ducklake.transaction() as tx:
        table = tx.table("table") if seeded else tx.create_table("table", {"x": dl.Int64()})
        table.add_column(dl.Column("temporary", dl.Int64()))
        table.remove_column("temporary")
    with dl.connect(catalog_url) as reopened:
        table = reopened.table("table")
        table.add_column(dl.Column("next", dl.Int64()))
        columns = table.schema.columns

    # Assert
    assert columns == [
        dl.Column("x", dl.Int64(), field_id=1),
        dl.Column("next", dl.Int64(), field_id=3),
    ]


@pytest.mark.parametrize("table_count", [3, 257])
def test_batched_renames_preserve_table_metadata(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, catalog_url: str, table_count: int
) -> None:
    # Arrange
    names = [f"table_{index}" for index in range(table_count)]
    with ducklake.transaction() as tx:
        for name in names:
            tx.create_table(name, {"x": dl.Int64()})
    metadata = sa.text(
        "SELECT table_id, table_uuid, schema_id, path, path_is_relative FROM ducklake_table "
        "WHERE end_snapshot IS NULL ORDER BY table_id"
    )
    with catalog_engine.connect() as connection:
        before = connection.execute(metadata).all()

    with ducklake.transaction() as tx:
        for name in names:
            tx.table(name).rename(f"{name}_previous")

    # Act
    with ducklake.transaction() as tx:
        for name in names:
            table = tx.table(f"{name}_previous")
            table.rename(f"{name}_temporary")
            table.rename(f"{name}_renamed")

    # Assert
    with catalog_engine.connect() as connection:
        assert connection.execute(metadata).all() == before
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM ducklake_table")) == 3 * len(names)
    with dl.connect(catalog_url) as reopened:
        for name in names:
            assert reopened.table(f"{name}_renamed").schema.columns == [
                dl.Column("x", dl.Int64(), field_id=1)
            ]


@pytest.fixture()
def column_stats_update_log(
    ducklake: dl.Ducklake, catalog_engine: sa.Engine, catalog: str
) -> None:
    table = ducklake.create_table("table", {"x": dl.Float64(), "y": dl.Float64()})
    table.write_data_files(
        [
            dl.WriteDataFile(
                "seed.parquet",
                statistics=dl.DataFileStatistics(
                    num_rows=10,
                    column_stats={
                        column_id: dl.ColumnStats(
                            min_value=0.0, max_value=10.0, null_count=0, contains_nan=False
                        )
                        for column_id in [1, 2]
                    },
                ),
            )
        ]
    )
    with catalog_engine.begin() as connection:
        connection.execute(sa.text("CREATE TABLE column_stats_updates (column_id BIGINT)"))
        if catalog == "postgres":
            connection.execute(
                sa.text(
                    "CREATE FUNCTION log_column_stats_update() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    "BEGIN INSERT INTO column_stats_updates VALUES (NEW.column_id); RETURN NEW; END $$"
                )
            )
            trigger_body = "EXECUTE FUNCTION log_column_stats_update()"
        elif catalog == "mysql":
            trigger_body = "INSERT INTO column_stats_updates VALUES (NEW.column_id)"
        else:
            trigger_body = "BEGIN INSERT INTO column_stats_updates VALUES (NEW.column_id); END"
        connection.execute(
            sa.text(
                "CREATE TRIGGER log_column_stats_update AFTER UPDATE ON ducklake_table_column_stats "
                f"FOR EACH ROW {trigger_body}"
            )
        )


@pytest.mark.parametrize(
    ("min_value", "max_value", "null_count", "contains_nan", "changed"),
    [
        (1.0, 9.0, 0, False, False),
        (-1.0, 9.0, 0, False, True),
        (1.0, 11.0, 0, False, True),
        (1.0, 9.0, 1, False, True),
        (1.0, 9.0, 0, True, True),
        (None, None, None, None, True),
    ],
    ids=["unchanged", "minimum", "maximum", "nulls", "nans", "unknown"],
)
def test_only_changed_column_statistics_are_updated(
    ducklake: dl.Ducklake,
    catalog_engine: sa.Engine,
    column_stats_update_log: None,
    min_value: float | None,
    max_value: float | None,
    null_count: int | None,
    contains_nan: bool | None,
    changed: bool,
) -> None:
    # Arrange
    statistics = dl.DataFileStatistics(
        num_rows=10,
        column_stats={
            1: dl.ColumnStats(
                min_value=min_value,
                max_value=max_value,
                null_count=null_count,
                contains_nan=contains_nan,
            ),
            2: dl.ColumnStats(min_value=1.0, max_value=9.0, null_count=0, contains_nan=False),
        },
    )

    # Act
    with ducklake.transaction() as tx:
        tx.table("table").write_data_files(
            [dl.WriteDataFile("first.parquet", statistics=statistics)]
        )
        tx.table("table").write_data_files(
            [dl.WriteDataFile("second.parquet", statistics=statistics)]
        )

    # Assert
    with catalog_engine.connect() as connection:
        updates = (
            connection.execute(sa.text("SELECT column_id FROM column_stats_updates"))
            .scalars()
            .all()
        )
        row_count = connection.scalar(sa.text("SELECT record_count FROM ducklake_table_stats"))
    assert updates == ([1] if changed else [])
    assert row_count == 30
