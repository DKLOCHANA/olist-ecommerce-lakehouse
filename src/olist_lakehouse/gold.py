"""Gold layer: star schema + business mart, defined as plain SQL files.

Each file in ``sql/gold`` is one model, run in file-name order. Inside a model,
``{{ silver.orders }}`` or ``{{ gold.dim_customer }}`` is replaced with the real
table name for the current environment (temp view locally, Unity Catalog table
on Databricks) - a tiny dbt-style ``ref()``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from olist_lakehouse.config import PROJECT_ROOT
from olist_lakehouse.sources import SOURCES
from olist_lakehouse.spark import TableStore

log = logging.getLogger(__name__)

GOLD_SQL_DIR = PROJECT_ROOT / "sql" / "gold"
REF_PATTERN = re.compile(r"\{\{\s*(bronze|silver|gold)\.(\w+)\s*\}\}")


def model_files(sql_dir: Path = GOLD_SQL_DIR) -> list[Path]:
    return sorted(sql_dir.glob("*.sql"))


def model_name(path: Path) -> str:
    """``05_fact_order_items.sql`` -> ``fact_order_items``."""
    return path.stem.split("_", 1)[1]


def render(sql: str, store: TableStore) -> str:
    return REF_PATTERN.sub(lambda m: store.sql_name(m.group(1), m.group(2)), sql)


def run(store: TableStore, sql_dir: Path = GOLD_SQL_DIR) -> dict[str, int]:
    for name in SOURCES:  # make silver tables queryable by name
        store.read("silver", name)

    counts: dict[str, int] = {}
    for path in model_files(sql_dir):
        name = model_name(path)
        df = store.spark.sql(render(path.read_text(), store))
        store.write(df, "gold", name)
        counts[name] = store.read("gold", name).count()
        log.info("gold.%s: %s rows", name, counts[name])
    return counts
