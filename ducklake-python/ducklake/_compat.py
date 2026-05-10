from typing import Any


class _DummyModule:  # pragma: no cover
    def __init__(self, module: str) -> None:
        self.module = module

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        raise ValueError(f"Module '{self.module}' is not installed.")


# -------------------------------------------- DUCKDB ------------------------------------------- #

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = _DummyModule("duckdb")  # ty: ignore[invalid-assignment]

# -------------------------------------------- POLARS ------------------------------------------- #

try:
    import polars
    from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS
except ImportError:  # pragma: no cover
    polars = _DummyModule("polars")  # ty: ignore[invalid-assignment]
    DEFAULT_QUERY_OPT_FLAGS = None  # ty: ignore[invalid-assignment]

# ------------------------------------------- PYARROW ------------------------------------------- #

try:
    import pyarrow
except ImportError:  # pragma: no cover
    pyarrow = _DummyModule("pyarrow")  # ty: ignore[invalid-assignment]

# ------------------------------------------ SQLALCHEMY ----------------------------------------- #

try:
    import sqlalchemy
except ImportError:  # pragma: no cover
    sqlalchemy = _DummyModule("sqlalchemy")  # ty: ignore[invalid-assignment]

# -------------------------------------------- EXPORT ------------------------------------------- #

__all__ = ["duckdb", "pyarrow", "sqlalchemy"]
