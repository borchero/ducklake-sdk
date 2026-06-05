import ducklake.exceptions as dlexc


def test_exceptions_all_exports() -> None:
    # Arrange
    expected_exports = [
        "AlreadyExistsError",
        "AlreadyInitializedError",
        "ImmutableDucklakeError",
        "InvalidCastError",
        "InvalidNullabilityChangeError",
        "InvalidNullValueError",
        "NotFoundError",
        "NotInitializedError",
        "OutdatedVersionError",
        "TransactionConflictError",
    ]

    # Act
    actual_exports = dlexc.__all__

    # Assert
    assert actual_exports == expected_exports
