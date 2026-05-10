import pytest

from ducklake._storage import S3StorageOptions, StorageOptionSet


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


# ------------------------------------- S3StorageOptions ---------------------------------------- #


def test_s3_options_from_env(
    clean_aws_env: None,  # noqa: ARG001
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


def test_s3_options_from_env_fallback_aliases(
    clean_aws_env: None,  # noqa: ARG001
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


def test_s3_options_from_env_empty(
    clean_aws_env: None,  # noqa: ARG001
) -> None:
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


# -------------------------------------- StorageOptionSet --------------------------------------- #


def test_storage_option_set_empty(
    clean_aws_env: None,  # noqa: ARG001
) -> None:
    # Act
    option_set = StorageOptionSet()

    # Assert
    assert option_set.options == []
    assert option_set.to_dict() == {}


def test_storage_option_set_user_options(
    clean_aws_env: None,  # noqa: ARG001
) -> None:
    # Act
    option_set = StorageOptionSet({"aws_access_key_id": "key", "aws_region": "us-east-1"})

    # Assert
    assert option_set.to_dict() == {"aws_access_key_id": "key", "aws_region": "us-east-1"}


def test_storage_option_set_merges_env_and_user(
    clean_aws_env: None,  # noqa: ARG001
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
