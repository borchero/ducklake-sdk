import datetime as dt
from typing import Any

import pytest

import ducklake as dl


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (dl.Column("id", dl.Int64()), "'id': int64"),
        (
            dl.Column(
                "created_at",
                dl.Timestamp(),
                nullable=False,
                tags={"role": "event_time"},
                initial_default="1970-01-01",
                default_value=("sql", "now()"),
                field_id=7,
            ),
            "'created_at': timestamp(microseconds) [not null, tags={'role': 'event_time'}, initial_default='1970-01-01', default_value=('sql', 'now()')] [field_id=7]",
        ),
        (
            dl.Schema({"id": dl.Int64(), "name": dl.Varchar()}),
            "'id': int64\n'name': varchar",
        ),
    ],
)
def test_schema_model_reprs(value: Any, expected: str) -> None:  # noqa: ANN401
    # Arrange
    subject = value

    # Act
    actual = repr(subject)

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (dl.PartitionColumn("country"), "country"),
        (dl.PartitionColumn("created_at", transform="month"), "created_at[month]"),
        (
            dl.PartitionColumn("user_id", transform="bucket", num_buckets=16),
            "user_id[bucket(16)]",
        ),
        (
            dl.Partitioning(
                [
                    dl.PartitionColumn("country"),
                    dl.PartitionColumn("created_at", transform="month"),
                    dl.PartitionColumn("user_id", transform="bucket", num_buckets=16),
                ]
            ),
            "country, created_at[month], user_id[bucket(16)]",
        ),
    ],
)
def test_partition_model_reprs(value: Any, expected: str) -> None:  # noqa: ANN401
    # Arrange
    subject = value

    # Act
    actual = repr(subject)

    # Assert
    assert actual == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (dl.Boolean(), "boolean"),
        (dl.Int64(), "int64"),
        (dl.UInt32(), "uint32"),
        (dl.Float32(), "float32"),
        (dl.Decimal(10, 2), "decimal(10, 2)"),
        (dl.Timestamp("milliseconds"), "timestamp(milliseconds)"),
        (dl.Varchar(), "varchar"),
        (dl.Uuid(), "uuid"),
        (dl.List(dl.Int64()), "list('element': int64)"),
        (
            dl.Struct({"a": dl.Int64(), "b": dl.Varchar()}),
            "struct('a': int64, 'b': varchar)",
        ),
        (
            dl.Map(dl.Varchar(), dl.Int64()),
            "map('key': varchar => 'value': int64)",
        ),
    ],
)
def test_data_type_reprs(value: dl.DataType, expected: str) -> None:
    # Arrange
    subject = value

    # Act
    actual = repr(subject)

    # Assert
    assert actual == expected


def test_snapshot_metadata_repr() -> None:
    # Arrange
    subject = dl.SnapshotMetadata()
    subject.id = 42
    subject.timestamp = dt.datetime(2024, 1, 2, 3, 4, 5, 678901)

    # Act
    actual = repr(subject)

    # Assert
    assert actual == "SnapshotMetadata(id=42, timestamp=2024-01-02T03:04:05.678901)"


def test_data_file_model_reprs() -> None:
    # Arrange
    column_stats = dl.ColumnStats(
        size_bytes=8,
        min_value=1,
        max_value=9,
        null_count=0,
        contains_nan=False,
    )
    statistics = dl.DataFileStatistics(
        10,
        file_size_bytes=100,
        footer_size_bytes=20,
        column_stats={1: column_stats},
    )
    delete_file = dl.DeleteFile(
        "delete/file.parquet",
        3,
        file_size_bytes=50,
        footer_size_bytes=12,
    )
    write_data_file = dl.WriteDataFile(
        "data/file.parquet",
        statistics=statistics,
        partition_values={"country": "DE"},
    )
    scan_data_file = dl.ScanDataFile(
        "data/file.parquet",
        statistics,
        [delete_file],
        None,
    )
    scan_result = dl.ScanResult([scan_data_file], [])

    # Act
    actual = {
        "column_stats": repr(column_stats),
        "statistics": repr(statistics),
        "delete_file": repr(delete_file),
        "write_data_file": repr(write_data_file),
        "scan_data_file": repr(scan_data_file),
        "scan_result": repr(scan_result),
    }

    # Assert
    assert actual == {
        "column_stats": "ColumnStats(size_bytes=8, min_value=1, max_value=9, null_count=0, contains_nan=False)",
        "statistics": "DataFileStatistics(num_rows=10, file_size_bytes=100, footer_size_bytes=20, column_stats={1: ColumnStats(size_bytes=8, min_value=1, max_value=9, null_count=0, contains_nan=False)})",
        "delete_file": "DeleteFile(path='delete/file.parquet', num_deletes=3, file_size_bytes=50, footer_size_bytes=12)",
        "write_data_file": "WriteDataFile(path='data/file.parquet', statistics=DataFileStatistics(num_rows=10, file_size_bytes=100, footer_size_bytes=20, column_stats={1: ColumnStats(size_bytes=8, min_value=1, max_value=9, null_count=0, contains_nan=False)}), partition_values={'country': 'DE'})",
        "scan_data_file": "ScanDataFile(path='data/file.parquet', statistics=DataFileStatistics(num_rows=10, file_size_bytes=100, footer_size_bytes=20, column_stats={1: ColumnStats(size_bytes=8, min_value=1, max_value=9, null_count=0, contains_nan=False)}), delete_files=[DeleteFile(path='delete/file.parquet', num_deletes=3, file_size_bytes=50, footer_size_bytes=12)], inline_deletes=None)",
        "scan_result": "ScanResult(data_files=[ScanDataFile(path='data/file.parquet', statistics=DataFileStatistics(num_rows=10, file_size_bytes=100, footer_size_bytes=20, column_stats={1: ColumnStats(size_bytes=8, min_value=1, max_value=9, null_count=0, contains_nan=False)}), delete_files=[DeleteFile(path='delete/file.parquet', num_deletes=3, file_size_bytes=50, footer_size_bytes=12)], inline_deletes=None)], inline_data=[])",
    }
