from ._native import (
    AlreadyExistsError,
    AlreadyInitializedError,
    InvalidCastError,
    InvalidNullabilityChangeError,
    InvalidNullValueError,
    MigrationConflictError,
    NotFoundError,
    NotInitializedError,
    OutdatedVersionError,
    ReadonlyDucklakeError,
    TransactionConflictError,
)


class MigrationError(Exception):
    """A migration could not be loaded or applied.

    The message names the affected migration. If the error was caused by another exception, e.g. an
    exception raised by the migration itself, it is available as the ``__cause__``. If the cause is
    a :class:`MigrationConflictError`, the migration was concurrently applied by another run.
    """


__all__ = [
    "AlreadyExistsError",
    "AlreadyInitializedError",
    "InvalidCastError",
    "InvalidNullabilityChangeError",
    "InvalidNullValueError",
    "MigrationConflictError",
    "MigrationError",
    "NotFoundError",
    "NotInitializedError",
    "OutdatedVersionError",
    "ReadonlyDucklakeError",
    "TransactionConflictError",
]
