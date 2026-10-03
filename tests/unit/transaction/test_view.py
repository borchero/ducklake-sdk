from typing import Literal

import pytest

import ducklake as dl


@pytest.mark.parametrize("if_not_exists", ["fail", "skip"])
def test_delete_view(
    shared_ducklake: dl.Ducklake,
    random_view_name: str,
    if_not_exists: Literal["fail", "skip"],
) -> None:
    # Arrange
    view = shared_ducklake.create_view(random_view_name, "SELECT 1 AS x")

    # Act
    with shared_ducklake.transaction() as tx:
        tx.delete_view(view.name, if_not_exists=if_not_exists)

    # Assert
    assert ("main", random_view_name) not in {item.name for item in shared_ducklake.list_views()}
