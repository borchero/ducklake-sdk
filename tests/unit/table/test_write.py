import polars as pl
import polars.exceptions as plexc
import pytest

import ducklake as dl
import ducklake.exceptions as dlexc


def test_writing_null_parquet_fails_for_non_nullable_column(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name, [dl.Column("x", dl.Int64(), nullable=False)]
    )
    lf = pl.LazyFrame({"x": [1, None, 3]})

    # Act & Assert
    with pytest.raises(plexc.SchemaError):
        table.sink_polars(lf)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
@pytest.mark.parametrize("nested", [False, True])
def test_writing_null_inline_fails_for_non_nullable_column(
    shared_ducklake: dl.Ducklake, random_table_name: str, nested: bool
) -> None:
    # Arrange
    column = dl.Column("x", dl.Int64(), nullable=False)
    df = pl.DataFrame({"x": [1, None, 3]})
    if nested:
        column = dl.Column("s", dl.Struct([column]))
        df = df.select(pl.struct("x").alias("s"))
    table = shared_ducklake.create_table(random_table_name, [column])

    # Act
    with pytest.raises(dlexc.InvalidNullValueError) as exc_info:
        table.write_polars(df)

    # Assert
    assert "non-nullable column 'x'" in str(exc_info.value)


def test_updating_nullability_fails_with_null_value_parquet(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    lf = pl.LazyFrame({"x": [1, None, 3]})
    table.sink_polars(lf)

    # Act & Assert
    with pytest.raises(dlexc.InvalidNullabilityChangeError):
        table.update_column_nullability("x", False)


@pytest.mark.skip_config(catalog="mysql", reason="Data inlining is not yet supported for MySQL.")
def test_updating_nullability_fails_with_null_value_inline(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})
    df = pl.DataFrame({"x": [1, None, 3]})
    table.write_polars(df)

    # Act & Assert
    with pytest.raises(dlexc.InvalidNullabilityChangeError):
        table.update_column_nullability("x", False)
