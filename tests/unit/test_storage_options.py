import pytest

from ducklake._storage import (
    AzureStorageOptions,
    GCSStorageOptions,
    S3StorageOptions,
    StorageOptionSet,
)


@pytest.fixture()
def clean_aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in [
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ]:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture()
def clean_gcs_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in [
        "GOOGLE_SERVICE_ACCOUNT_KEY",
        "GOOGLE_SERVICE_ACCOUNT",
    ]:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture()
def clean_azure_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in [
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_STORAGE_ENDPOINT",
    ]:
        monkeypatch.delenv(var, raising=False)


# ------------------------------------- S3StorageOptions ---------------------------------------- #


@pytest.mark.usefixtures("clean_aws_env")
def test_s3_options_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://s3.example.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    # Act
    options = S3StorageOptions.from_env()

    # Assert
    assert options.endpoint_url == "https://s3.example.com"
    assert options.access_key_id == "key"
    assert options.secret_access_key == "secret"
    assert options.region == "us-east-1"


@pytest.mark.usefixtures("clean_aws_env")
def test_s3_options_from_env_fallback_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("AWS_ENDPOINT", "https://alt.example.com")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")

    # Act
    options = S3StorageOptions.from_env()

    # Assert
    assert options.endpoint_url == "https://alt.example.com"
    assert options.region == "eu-west-1"


@pytest.mark.usefixtures("clean_aws_env")
def test_s3_options_from_env_empty() -> None:
    # Act
    options = S3StorageOptions.from_env()

    # Assert
    assert options.to_dict() == {}


def test_s3_options_from_dict() -> None:
    # Act
    options = S3StorageOptions.from_dict(
        {
            "aws_endpoint_url": "https://s3.example.com",
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",
            "aws_region": "us-east-1",
            "ignored_key": "value",
        }
    )

    # Assert
    assert options.endpoint_url == "https://s3.example.com"
    assert options.access_key_id == "key"
    assert options.secret_access_key == "secret"
    assert options.region == "us-east-1"


def test_s3_options_to_dict_filters_none() -> None:
    # Arrange
    options = S3StorageOptions(access_key_id="key", region=None)

    # Act
    result = options.to_dict()

    # Assert
    assert result == {"aws_access_key_id": "key"}


def test_s3_options_merge_overrides_only_when_set() -> None:
    # Arrange
    base = S3StorageOptions(endpoint_url="env", access_key_id="env_key", region="env_region")
    override = S3StorageOptions(access_key_id="user_key")

    # Act
    merged = base.merge(override)

    # Assert
    assert merged.endpoint_url == "env"
    assert merged.access_key_id == "user_key"
    assert merged.region == "env_region"
    assert merged.secret_access_key is None


# ------------------------------------- GCSStorageOptions --------------------------------------- #


@pytest.mark.usefixtures("clean_gcs_env")
def test_gcs_options_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_KEY", '{"key": "value"}')
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT", "sa@example.com")

    # Act
    options = GCSStorageOptions.from_env()

    # Assert
    assert options.service_account_key == '{"key": "value"}'
    assert options.service_account == "sa@example.com"


@pytest.mark.usefixtures("clean_gcs_env")
def test_gcs_options_from_env_empty() -> None:
    # Act
    options = GCSStorageOptions.from_env()

    # Assert
    assert options.to_dict() == {}


def test_gcs_options_from_dict() -> None:
    # Act
    options = GCSStorageOptions.from_dict(
        {
            "google_service_account_key": '{"key": "value"}',
            "google_service_account": "sa@example.com",
            "ignored_key": "value",
        }
    )

    # Assert
    assert options.service_account_key == '{"key": "value"}'
    assert options.service_account == "sa@example.com"


def test_gcs_options_to_dict_filters_none() -> None:
    # Arrange
    options = GCSStorageOptions(service_account="sa@example.com", service_account_key=None)

    # Act
    result = options.to_dict()

    # Assert
    assert result == {"google_service_account": "sa@example.com"}


def test_gcs_options_merge_overrides_only_when_set() -> None:
    # Arrange
    base = GCSStorageOptions(service_account_key="env_key", service_account="env_sa")
    override = GCSStorageOptions(service_account="user_sa")

    # Act
    merged = base.merge(override)

    # Assert
    assert merged.service_account_key == "env_key"
    assert merged.service_account == "user_sa"


# -------------------------------------- StorageOptionSet --------------------------------------- #


@pytest.mark.usefixtures("clean_aws_env", "clean_gcs_env", "clean_azure_env")
def test_storage_option_set_empty() -> None:
    # Act
    option_set = StorageOptionSet()

    # Assert
    assert option_set.options == []
    assert option_set.to_dict() == {}


@pytest.mark.usefixtures("clean_aws_env", "clean_gcs_env", "clean_azure_env")
def test_storage_option_set_user_options() -> None:
    # Act
    option_set = StorageOptionSet({"aws_access_key_id": "key", "aws_region": "us-east-1"})

    # Assert
    assert option_set.to_dict() == {"aws_access_key_id": "key", "aws_region": "us-east-1"}


@pytest.mark.usefixtures("clean_aws_env", "clean_gcs_env", "clean_azure_env")
def test_storage_option_set_merges_env_and_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env_key")
    monkeypatch.setenv("AWS_REGION", "env_region")

    # Act
    option_set = StorageOptionSet({"aws_access_key_id": "user_key"})

    # Assert: user overrides env, but env-only values are preserved
    assert option_set.to_dict() == {
        "aws_access_key_id": "user_key",
        "aws_region": "env_region",
    }


# ----------------------------------- AzureStorageOptions --------------------------------------- #


def test_azure_options_from_env(
    clean_azure_env: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "myaccount")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "mykey")
    monkeypatch.setenv("AZURE_STORAGE_ENDPOINT", "http://localhost:10000")

    # Act
    options = AzureStorageOptions.from_env()

    # Assert
    assert options.account_name == "myaccount"
    assert options.account_key == "mykey"
    assert options.endpoint_url == "http://localhost:10000"


def test_azure_options_from_env_empty(
    clean_azure_env: None,  # noqa: ARG001
) -> None:
    # Act
    options = AzureStorageOptions.from_env()

    # Assert
    assert options.to_dict() == {}


def test_azure_options_from_dict() -> None:
    # Act
    options = AzureStorageOptions.from_dict(
        {
            "azure_storage_account_name": "myaccount",
            "azure_storage_account_key": "mykey",
            "azure_storage_endpoint": "http://localhost:10000",
            "ignored_key": "value",
        }
    )

    # Assert
    assert options.account_name == "myaccount"
    assert options.account_key == "mykey"
    assert options.endpoint_url == "http://localhost:10000"


def test_azure_options_to_dict_filters_none() -> None:
    # Arrange
    options = AzureStorageOptions(account_name="myaccount", account_key=None)

    # Act
    result = options.to_dict()

    # Assert
    assert result == {"azure_storage_account_name": "myaccount"}


def test_azure_options_merge_overrides_only_when_set() -> None:
    # Arrange
    base = AzureStorageOptions(
        account_name="env_account", account_key="env_key", endpoint_url="env_endpoint"
    )
    override = AzureStorageOptions(account_key="user_key")

    # Act
    merged = base.merge(override)

    # Assert
    assert merged.account_name == "env_account"
    assert merged.account_key == "user_key"
    assert merged.endpoint_url == "env_endpoint"
