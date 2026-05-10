from collections.abc import Sequence
from typing import Any

import duckdb

from ducklake import MaintenanceResult, TableName


def fetch_result_dicts(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: Sequence[Any] | None = None,
) -> list[dict[str, Any]]:
    cursor = (
        connection.execute(query) if parameters is None else connection.execute(query, parameters)
    )
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def build_named_query_params(**kwargs: Any) -> tuple[str, list[Any]]:  # noqa: ANN401
    """Build named parameters for use in DuckDB queries."""
    parts: list[str] = []
    args: list[Any] = []
    for name, value in kwargs.items():
        if value is None:
            continue
        parts.append(f"{name} => ?")
        args.append(value)
    return ((", " + ", ".join(parts)) if parts else "", args)


# ------------------------------------------- RESULTS ------------------------------------------- #


def parse_cleanup_path_result(rows: list[dict[str, Any]]) -> list[str]:
    """Parse the result of a cleanup query into a more user-friendly format."""
    return [row["path"] for row in rows]


def parse_maintenance_result(rows: list[dict[str, Any]]) -> list[MaintenanceResult]:
    """Parse the result of a maintenance query into a more user-friendly format."""
    return [
        {
            "table_name": TableName(row["schema_name"], row["table_name"]),
            "files_processed": row["files_processed"],
            "files_created": row["files_created"],
        }
        for row in rows
    ]
