"""End-to-end: generate sample data, run every layer, check results and idempotency."""

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from generate_sample_data import generate
from olist_lakehouse import bronze, gold, quality, silver


@pytest.fixture(scope="module")
def built(module_store):
    """Build the lakehouse once; the tests below run in order against it."""
    store = module_store
    generate(Path(store.settings.raw_path), n_orders=300, seed=7)
    bronze.run(store.spark, store, store.settings.raw_path, "20240101T000000Z")
    silver_stats = silver.run(store)
    gold_counts = gold.run(store)
    return store, silver_stats, gold_counts


def test_full_pipeline(built):
    store, silver_stats, gold_counts = built

    assert silver_stats["customers"]["quarantined"] == 2  # two invalid states
    assert silver_stats["orders"]["quarantined"] == 2  # two unknown statuses
    assert silver_stats["order_items"]["quarantined"] == 2  # negative + unparseable price
    assert gold_counts["fact_order_items"] > 0
    assert gold_counts["dim_date"] > 300

    results = quality.run(store)
    failed = [r for r in results if not r.passed and r.severity == "error"]
    assert failed == []
    assert store.read("gold", "dq_results").count() == len(results)


def test_rerun_is_idempotent(built):
    store, silver_stats, _ = built
    bronze.run(store.spark, store, store.settings.raw_path, "20240102T000000Z")
    rerun = silver.run(store)
    assert rerun["orders"]["valid"] == silver_stats["orders"]["valid"]
    assert store.read("silver", "orders").count() == silver_stats["orders"]["valid"]
    # bronze keeps history of both batches
    assert store.read("bronze", "orders").select("_batch_id").distinct().count() == 2


def test_quality_fails_on_broken_reference(built):
    store, _, _ = built
    dim = store.read("gold", "dim_product")
    store.write(dim.filter(F.col("product_key") != -1).limit(5), "gold", "dim_product")
    with pytest.raises(quality.DataQualityError, match="relationship"):
        quality.run(store)
