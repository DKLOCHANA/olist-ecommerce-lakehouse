import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture(scope="session")
def spark():
    from olist_lakehouse.spark import get_spark

    session = get_spark("olist-tests")
    yield session
    session.stop()


def make_store(spark, base):
    from olist_lakehouse.config import Settings
    from olist_lakehouse.spark import TableStore

    settings = Settings(
        env="local",
        raw_path=str(base / "raw"),
        lakehouse_path=str(base / "lakehouse"),
        catalog="test",
    )
    return TableStore(spark, settings)


@pytest.fixture(scope="module")
def module_store(spark, tmp_path_factory):
    return make_store(spark, tmp_path_factory.mktemp("lakehouse"))
