from __future__ import annotations

import os
import sys
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    import duckdb


# -------------------------------------- STORAGE OPTION SET ------------------------------------- #


class StorageOptionSet:
    """A set of storage options for different storage backends."""

    def __init__(self, user_options: dict[str, str] | None = None) -> None:
        all_options: list[StorageOptions] = []

        # S3
        s3_options_env = S3StorageOptions.from_env()
        s3_options_user = S3StorageOptions.from_dict(user_options or {})
        s3_options = s3_options_env.merge(s3_options_user)
        if s3_options.to_dict():
            all_options.append(s3_options)

        # GCS
        gcs_options_env = GCSStorageOptions.from_env()
        gcs_options_user = GCSStorageOptions.from_dict(user_options or {})
        gcs_options = gcs_options_env.merge(gcs_options_user)
        if gcs_options.to_dict():
            all_options.append(gcs_options)

        self.options = all_options

    def to_dict(self) -> dict[str, str]:
        options_dict = {}
        for options in self.options:
            options_dict.update(options.to_dict())
        return options_dict

    def apply_to_duckdb_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        for options in self.options:
            options.apply_to_duckdb_connection(connection)


# ----------------------------------------------------------------------------------------------- #
#                                         STORAGE OPTIONS                                         #
# ----------------------------------------------------------------------------------------------- #


@dataclass
class StorageOptions(ABC):
    """Abstract base class for types holding configuration for file storage backends."""

    @classmethod
    @abstractmethod
    def from_env(cls) -> StorageOptions:
        """Create an instance of the storage options by reading from environment variables."""

    @classmethod
    @abstractmethod
    def from_dict(cls, options: dict[str, str]) -> StorageOptions:
        """Parse the storage options from a dictionary of options."""

    @abstractmethod
    def to_dict(self) -> dict[str, str]:
        """Convert the options to a dictionary that can be passed to the Rust backend."""

    @abstractmethod
    def apply_to_duckdb_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Apply the storage options to the given DuckDB connection."""

    def merge(self, other: Self) -> Self:
        """Merge another instance of the same storage options."""
        args = {**asdict(self), **{k: v for k, v in asdict(other).items() if v is not None}}
        return self.__class__(**args)


# ---------------------------------------------- S3 --------------------------------------------- #


@dataclass(kw_only=True)
class S3StorageOptions(StorageOptions):
    """Storage options for S3-compatible storage backends."""

    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str | None = None

    @classmethod
    def from_env(cls) -> S3StorageOptions:
        return cls(
            endpoint_url=os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT"),
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
        )

    @classmethod
    def from_dict(cls, options: dict[str, str]) -> S3StorageOptions:
        return cls(
            endpoint_url=options.get("aws_endpoint_url"),
            access_key_id=options.get("aws_access_key_id"),
            secret_access_key=options.get("aws_secret_access_key"),
            region=options.get("aws_region"),
        )

    def to_dict(self) -> dict[str, str]:
        options = {}
        if self.endpoint_url is not None:
            options["aws_endpoint_url"] = self.endpoint_url
        if self.access_key_id is not None:
            options["aws_access_key_id"] = self.access_key_id
        if self.secret_access_key is not None:
            options["aws_secret_access_key"] = self.secret_access_key
        if self.region is not None:
            options["aws_region"] = self.region
        return options

    def apply_to_duckdb_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        options = []
        if self.endpoint_url is not None:
            url = urlparse(self.endpoint_url)
            options.append(f"ENDPOINT '{url.netloc}'")
            if (
                url.hostname is not None
                and url.hostname.endswith("amazonaws.com")
                and "s3" in url.hostname
            ):
                options.append("URL_STYLE 'vhost'")
            else:
                options.append("URL_STYLE 'path'")
            if url.scheme == "http":
                options.append("USE_SSL 'false'")
        if self.access_key_id is not None:
            options.append(f"KEY_ID '{self.access_key_id}'")
        if self.secret_access_key is not None:
            options.append(f"SECRET '{self.secret_access_key}'")
        if self.region is not None:
            options.append(f"REGION '{self.region}'")

        if options:
            connection.execute("INSTALL httpfs;")
            connection.execute(
                f"CREATE OR REPLACE SECRET s3_credentials (TYPE S3, {', '.join(options)});"
            )


# ---------------------------------------------- GCS --------------------------------------------- #


@dataclass(kw_only=True)
class GCSStorageOptions(StorageOptions):
    """Storage options for Google Cloud Storage."""

    endpoint_url: str | None = None
    service_account: str | None = None

    @classmethod
    def from_env(cls) -> GCSStorageOptions:
        return cls(
            endpoint_url=os.getenv("STORAGE_EMULATOR_HOST"),
            service_account=os.getenv("GOOGLE_SERVICE_ACCOUNT"),
        )

    @classmethod
    def from_dict(cls, options: dict[str, str]) -> GCSStorageOptions:
        return cls(
            endpoint_url=options.get("gcp_endpoint_url"),
            service_account=options.get("gcp_service_account"),
        )

    def to_dict(self) -> dict[str, str]:
        options = {}
        if self.endpoint_url is not None:
            options["gcp_endpoint_url"] = self.endpoint_url
        if self.service_account is not None:
            options["gcp_service_account"] = self.service_account
        return options

    def apply_to_duckdb_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        options = []
        if self.endpoint_url is not None:
            options.append(f"ENDPOINT '{self.endpoint_url}'")
        if self.service_account is not None:
            options.append(f"ACCOUNT '{self.service_account}'")

        if options:
            connection.execute("INSTALL httpfs;")
            connection.execute(
                f"CREATE OR REPLACE SECRET gcs_credentials (TYPE GCS, {', '.join(options)});"
            )
