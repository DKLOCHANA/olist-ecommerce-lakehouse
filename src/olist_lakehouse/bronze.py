"""Bronze layer: land raw CSV files as Delta tables, unchanged.

Extract + Load of the ELT pattern. Every column is kept as a string so a bad
value never breaks ingestion; audit columns record where and when each row came
from. Each run is appended with a new ``_batch_id`` so history is preserved.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from olist_lakehouse.sources import SOURCES, Source
from olist_lakehouse.spark import TableStore

log = logging.getLogger(__name__)


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def read_raw(spark: SparkSession, raw_path: str, source: Source) -> DataFrame:
    schema = StructType([StructField(c, StringType()) for c in source.columns])
    return (
        spark.read.option("header", "true")
        .option("multiLine", "true")  # review comments contain line breaks
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(f"{raw_path}/{source.file}")
    )


def add_audit_columns(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def run(spark: SparkSession, store: TableStore, raw_path: str, batch_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for source in SOURCES.values():
        df = add_audit_columns(read_raw(spark, raw_path, source), batch_id)
        store.write(df, "bronze", source.name, mode="append")
        counts[source.name] = store.read("bronze", source.name).filter(F.col("_batch_id") == batch_id).count()
        log.info("bronze.%s: %s rows landed (batch %s)", source.name, counts[source.name], batch_id)
    return counts
