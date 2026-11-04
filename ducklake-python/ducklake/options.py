from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    import datetime as dt


class DatabaseOptions(TypedDict, total=False):
    """Configure the connection pool for the catalog database."""

    #: Maximum connections in the pool. Omitted or `None` selects four for PostgreSQL/MySQL
    #: and one for SQLite.
    max_connections: int | None
    #: Minimum idle connections to maintain. Defaults to zero.
    min_connections: int
    #: Maximum time to acquire a connection, including connection establishment and health
    #: checks. Defaults to 30 seconds.
    acquire_timeout: dt.timedelta
    #: Check idle connections for liveness before acquiring them. Defaults to `True`.
    test_before_acquire: bool
    #: Close idle connections during periodic pool cleanup, subject to `min_connections`.
    #: Defaults to ten minutes. `None` disables idle expiry.
    idle_timeout: dt.timedelta | None
    #: Maximum connection lifetime before recycling. Defaults to 30 minutes. `None` disables
    #: recycling. Connections in use are not interrupted.
    max_lifetime: dt.timedelta | None
