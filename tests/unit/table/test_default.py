import datetime as dt
import decimal
import uuid
from typing import Any

import pytest

import ducklake as dl


@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        (dl.Boolean(), True),
        (dl.Int64(), 42),
        (dl.UInt64(), 2**63),
        (dl.Float64(), 1.25),
        (dl.Varchar(), "hello"),
        (dl.Blob(), b"hello"),
        (dl.Decimal(4, 2), decimal.Decimal("12.34")),
        (dl.Uuid(), uuid.UUID("12345678-1234-5678-1234-567812345678")),
        (dl.Date(), dt.date(2024, 1, 2)),
        (dl.Time(), dt.time(12, 34, 56, 789)),
        (dl.Timestamp(), dt.datetime(2024, 1, 2, 12, 34, 56, 789)),
    ],
)
def test_value_default_roundtrip(
    shared_ducklake: dl.Ducklake,
    random_table_name: str,
    dtype: dl.DataType,
    value: Any,  # noqa: ANN401
) -> None:
    # Arrange
    table = shared_ducklake.create_table(
        random_table_name,
        [dl.Column("x", dtype, default_value=value)],
    )

    # Act
    roundtripped = table.schema.columns[0].default_value

    # Assert
    assert roundtripped == value
