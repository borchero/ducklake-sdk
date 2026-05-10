from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import quote, unquote

from . import _native as native
from ._storage import StorageOptionSet
from .ducklake import Ducklake

if TYPE_CHECKING:
    import sqlalchemy as sa


def create(
    catalog_url: str | sa.URL, *, data_path: str, storage_options: dict[str, str] | None = None
) -> Ducklake:
    """Create a new DuckLake by initializing a new catalog database.

    Currently, supported catalog databases are PostgreSQL, MySQL, and SQLite.

    Args:
        catalog_url: The URL of the catalog database. This may either be a string or a `URL`
            object from `sqlalchemy`. If a string is provided, the URL must be
            `sqlalchemy`-compatible.
        data_path: The root path where data files should be stored. This may be a local path
            (including NFS paths) or a cloud storage path for S3, GCS, or Azure Blob Storage.
            See also: https://ducklake.select/docs/stable/duckdb/usage/choosing_storage.
        storage_options: Optional dictionary of storage options. These may be provided to connect
            to cloud storage services. If not provided, storage options will be inferred from
            environment variables.

    Returns:
        A `Ducklake` instance that can be used to interact with the DuckLake.

    Raises:
        AlreadyInitializedError: If the catalog database is already initialized. In this case, call
            :meth:`connect` instead.
    """
    connection_args = _sanitize_url(catalog_url)
    storage_option_set = StorageOptionSet(storage_options)
    pyducklake = native.create(
        connection_args._native(),
        data_path,
        list(storage_option_set.to_dict().items()),
    )
    return Ducklake._from_pyducklake(pyducklake, connection_args, storage_option_set)


def connect(
    catalog_url: str | sa.URL,
    *,
    at: int | dt.datetime | None = None,
    migrate: bool = False,
    storage_options: dict[str, str] | None = None,
) -> Ducklake:
    """Connect to an existing DuckLake by connecting to its catalog database.

    Currently, supported catalog databases are PostgreSQL, MySQL, and SQLite.

    Args:
        catalog_url: The URL of the catalog database. This may either be a string or a `URL`
            object from `sqlalchemy`. If a string is provided, the URL must be
            `sqlalchemy`-compatible.
        at: Optional argument to specify a historical snapshot to connect to. This may either be
            a snapshot ID (int) or a snapshot timestamp (datetime). If not provided, the
            connection will be made to the latest snapshot. If provided, the connection will be
            read-only. Trying to make any modifications will raise an exception.
        migrate: Whether to automatically migrate to the latest supported catalog version if the
            catalog database is still on an older version.
        storage_options: Optional dictionary of storage options. These may be provided to connect
            to cloud storage services. If not provided, storage options will be inferred from
            environment variables.

    Returns:
        A `Ducklake` instance that can be used to interact with the DuckLake.

    Raises:
        NotInitializedError: If the catalog database is not yet initialized. In this case, call
            :meth:`create` first.
    """
    connection_args = _sanitize_url(catalog_url)
    storage_option_set = StorageOptionSet(storage_options)
    pyducklake = native.connect(
        connection_args._native(),
        snapshot_id=at if isinstance(at, int) else None,
        snapshot_timestamp=at if isinstance(at, dt.datetime) else None,
        migrate=migrate,
        storage_options=list(storage_option_set.to_dict().items()),
    )
    return Ducklake._from_pyducklake(pyducklake, connection_args, storage_option_set)


# ------------------------------------------- PARSING ------------------------------------------- #


def _sanitize_url(url: str | sa.URL) -> ConnectionArgs:
    if isinstance(url, str):
        str_url = url
    else:
        str_url = url.render_as_string(hide_password=False)
    return ConnectionArgs.parse(str_url)


# --------------------------------------- CONNECTION ARGS --------------------------------------- #

# NOTE: Much of the parsing logic below is copied and adapted from
#  https://github.com/sqlalchemy/sqlalchemy/blob/2e058575c237f33058da79433f6f1ce5f6739121/lib/sqlalchemy/engine/url.py


URL_PATTERN = re.compile(
    r"""(?P<name>[\w\+]+)://
    (?:
        (?P<username>[^:/]*)
        (?::(?P<password>[^@]*))?
    @)?
    (?:
        (?:
            \[(?P<ipv6host>[^/\?]+)\] |
            (?P<ipv4host>[^/:\?]+)
        )?
        (?::(?P<port>[^/\?]*))?
    )?
    (?:/(?P<database>[^\?]*))?
    (?:\?(?P<query>.*))?""",
    re.X,
)


@dataclass
class ConnectionArgs:
    dialect: Literal["postgresql", "mysql", "sqlite"]
    username: str | None
    password: str | None
    host: str | None
    port: int | None
    database: str | None
    query: str | None

    @classmethod
    def parse(cls, url: str) -> ConnectionArgs:
        match = URL_PATTERN.match(url)
        if match is not None:
            components = match.groupdict()
            dialect = cast(str, components["name"]).split("+", 1)[0]
            if dialect not in ("postgresql", "mysql", "sqlite"):
                raise ValueError(f"Dialect '{dialect}' is currently not supported.")

            return ConnectionArgs(
                dialect=dialect,  # ty: ignore[invalid-argument-type]
                username=(unquote(components["username"]) if components["username"] else None),
                password=(unquote(components["password"]) if components["password"] else None),
                host=components["ipv4host"] or components["ipv6host"],
                port=int(components["port"]) if components["port"] else None,
                database=components["database"],
                query=components["query"],
            )
        else:
            raise ValueError(f"Connection URL '{url}' is malformed.")

    def __post_init__(self) -> None:
        if self.dialect != "sqlite" and self.host is None:
            raise ValueError(f"Host must be provided for '{self.dialect}'.")
        if self.dialect == "sqlite" and self.database is None:
            raise ValueError(f"Database must be provided for '{self.dialect}'.")

    def _native(self) -> str:
        """Obtain the "native" string representation.

        This is different to the Python string representation as Rust's sqlx uses one slash fewer
        for SQLite URLs.
        """
        result = str(self)
        if self.dialect == "sqlite":
            result = result.replace("sqlite:///", "sqlite://", 1)
        return result

    def __str__(self) -> str:
        match self.dialect:
            case "postgresql" | "mysql":
                url = self.dialect + "://"
                if self.username is not None:
                    url += quote(self.username, safe=" +")
                    if self.password is not None:
                        url += ":" + quote(str(self.password), safe=" +")
                    url += "@"
                if self.host is not None:
                    if ":" in self.host:
                        url += f"[{self.host}]"
                    else:
                        url += self.host
                if self.port is not None:
                    url += ":" + str(self.port)
                if self.database is not None:
                    url += "/" + self.database
                if self.query is not None:
                    url += "?" + self.query
                return url
            case "sqlite":
                url = f"sqlite:///{self.database}"
                if self.query is not None:
                    url += f"?{self.query}"
                return url
