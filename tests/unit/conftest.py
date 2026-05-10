import uuid

import pytest


@pytest.fixture()
def random_schema_name() -> str:
    return "schema_" + str(uuid.uuid4()).replace("-", "")


@pytest.fixture()
def random_table_name() -> str:
    return "table_" + str(uuid.uuid4()).replace("-", "")
