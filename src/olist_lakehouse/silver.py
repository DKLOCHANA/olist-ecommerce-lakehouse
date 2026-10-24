"""Silver layer: clean, type, standardise, deduplicate and validate.

* Strings are trimmed and empty strings become NULL.
* Columns are cast with ``try_cast``; a value that fails to cast is flagged.
* Codes are standardised (lower-case statuses, upper-case states, title-case cities).
* Duplicates on the business key keep the most recently ingested row.
* Rows that break a row-level rule go to ``<table>_quarantine`` with the reason,
  instead of silently disappearing or breaking the pipeline.
* Valid rows are MERGEd into silver, so re-running a batch is idempotent.
"""

from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from olist_lakehouse.sources import (
    BRAZIL_STATES,
    ORDER_STATUSES,
    PAYMENT_TYPES,
    SOURCES,
    Source,
)
from olist_lakehouse.spark import TableStore

log = logging.getLogger(__name__)

AUDIT_COLUMNS = ["_ingested_at", "_batch_id"]


def clean_strings(df: DataFrame, columns: list[str]) -> DataFrame:
    for c in columns:
        trimmed = F.trim(F.col(c))
        df = df.withColumn(c, F.when(trimmed == "", None).otherwise(trimmed))
    return df


def cast_columns(df: DataFrame, source: Source) -> DataFrame:
    """Cast to the typed schema; keep a list of columns whose value failed to cast."""
    typed = {c: t for c, t in source.columns.items() if t != "string"}
    casts = {c: F.expr(f"try_cast(`{c}` AS {t})") for c, t in typed.items()}
    # failures must be computed from the raw strings, before they are replaced
    failures = [F.when(F.col(c).isNotNull() & cast.isNull(), F.lit(c)) for c, cast in casts.items()]
    flags = F.array_compact(F.array(*failures)) if failures else F.array().cast("array<string>")
    df = df.withColumn("_cast_failures", flags)
    return df.withColumns(casts) if casts else df  # Spark Connect rejects an empty mapping


def standardise(df: DataFrame, source: Source) -> DataFrame:
    for c in source.columns:
        if c.endswith("_state"):
            df = df.withColumn(c, F.upper(c))
        elif c.endswith("_city"):
            df = df.withColumn(c, F.initcap(F.lower(c)))
        elif c in {"order_status", "payment_type", "product_category_name"}:
            df = df.withColumn(c, F.lower(c))
    return df


def deduplicate(df: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def row_rules(source: Source) -> dict[str, Column]:
    """Row-level validation rules: name -> condition that must be TRUE for a valid row."""
    rules: dict[str, Column] = {f"{k}_not_null": F.col(k).isNotNull() for k in source.keys}
    rules["types_valid"] = F.size("_cast_failures") == 0

    if source.name == "orders":
        rules["status_accepted"] = F.col("order_status").isin(*ORDER_STATUSES)
        rules["purchase_ts_not_null"] = F.col("order_purchase_timestamp").isNotNull()
        rules["delivery_after_purchase"] = F.coalesce(
            F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp"),
            F.lit(True),
        )
    elif source.name == "order_items":
        rules["price_non_negative"] = F.coalesce(F.col("price") >= 0, F.lit(False))
        rules["freight_non_negative"] = F.coalesce(F.col("freight_value") >= 0, F.lit(True))
    elif source.name == "order_payments":
        rules["payment_type_accepted"] = F.col("payment_type").isin(*PAYMENT_TYPES)
        rules["payment_value_non_negative"] = F.coalesce(F.col("payment_value") >= 0, F.lit(False))
    elif source.name == "order_reviews":
        rules["score_between_1_and_5"] = F.col("review_score").between(1, 5)
    elif source.name in {"customers", "sellers"}:
        state = f"{source.name[:-1]}_state"
        rules["state_valid"] = F.col(state).isin(*BRAZIL_STATES)
    return rules


def split_valid_invalid(df: DataFrame, source: Source) -> tuple[DataFrame, DataFrame]:
    rules = row_rules(source)
    failed = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    df = df.withColumn("_dq_failures", F.array_compact(F.array(*failed)))
    valid = df.filter(F.size("_dq_failures") == 0).drop("_dq_failures", "_cast_failures")
    invalid = df.filter(F.size("_dq_failures") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid


def transform(bronze_df: DataFrame, source: Source) -> tuple[DataFrame, DataFrame]:
    cols = list(source.columns)
    df = bronze_df.select(*cols, *AUDIT_COLUMNS)
    df = clean_strings(df, cols)
    df = standardise(df, source)
    df = cast_columns(df, source)
    df = deduplicate(df, source.keys)
    return split_valid_invalid(df, source)


def latest_batch(df: DataFrame) -> DataFrame:
    last = df.agg(F.max("_batch_id")).first()[0]
    return df.filter(F.col("_batch_id") == last)


def run(store: TableStore) -> dict[str, dict[str, int]]:
    stats: dict[str, dict[str, int]] = {}
    for source in SOURCES.values():
        bronze = latest_batch(store.read("bronze", source.name))
        valid, invalid = transform(bronze, source)
        valid = valid.cache()
        store.upsert(valid, "silver", source.name, source.keys)
        store.write(invalid, "silver", f"{source.name}_quarantine", mode="append")
        stats[source.name] = {"valid": valid.count(), "quarantined": invalid.count()}
        valid.unpersist()
        log.info("silver.%s: %s", source.name, stats[source.name])
    return stats


def total(stats: dict[str, dict[str, int]], key: str) -> int:
    return reduce(lambda acc, s: acc + s[key], stats.values(), 0)
