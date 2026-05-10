import ducklake as dl


def test_default_table_metadata(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Assert
    assert table.metadata["data_inlining_row_limit"] == 10
    assert table.metadata["parquet_compression"] == "snappy"


def test_table_metadata_overwrite(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.set_metadata(data_inlining_row_limit=20)

    # Assert
    assert table.metadata["data_inlining_row_limit"] == 20


def test_table_metadata_reset(shared_ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.set_metadata(data_inlining_row_limit=20)
    table.set_metadata(data_inlining_row_limit=None)

    # Assert
    assert table.metadata["data_inlining_row_limit"] == 10


def test_table_metadata_via_schema(
    shared_ducklake: dl.Ducklake, random_table_name: str, random_schema_name: str
) -> None:
    # Arrange
    shared_ducklake.create_schema(random_schema_name)
    table = shared_ducklake.create_table(
        (random_schema_name, random_table_name), {"x": dl.Int64()}
    )
    other_table = shared_ducklake.create_table(random_table_name + "_other", {"x": dl.Int64()})

    # Act
    shared_ducklake.set_metadata(data_inlining_row_limit=20, schema=random_schema_name)

    # Assert
    assert table.metadata["data_inlining_row_limit"] == 20
    assert other_table.metadata["data_inlining_row_limit"] == 10


def test_global_metadata(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    ducklake.set_metadata(data_inlining_row_limit=25)

    # Assert
    assert table.metadata["data_inlining_row_limit"] == 25


def test_global_metadata_reset(ducklake: dl.Ducklake, random_table_name: str) -> None:
    # Arrange
    table = ducklake.create_table(random_table_name, {"x": dl.Int64()})
    default = table.metadata["data_inlining_row_limit"]
    ducklake.set_metadata(data_inlining_row_limit=25)

    # Act
    ducklake.set_metadata(data_inlining_row_limit=None)

    # Assert
    assert table.metadata["data_inlining_row_limit"] == default


def test_table_metadata_compression_setting(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.set_metadata(parquet_compression="zstd", parquet_compression_level=3)

    # Assert
    assert table.metadata["parquet_compression"] == "zstd"
    assert table.metadata["parquet_compression_level"] == 3


def test_table_metadata_boolean_setting(
    shared_ducklake: dl.Ducklake, random_table_name: str
) -> None:
    # Arrange
    table = shared_ducklake.create_table(random_table_name, {"x": dl.Int64()})

    # Act
    table.set_metadata(auto_compact=False)

    # Assert
    assert table.metadata["auto_compact"] is False
