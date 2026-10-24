"""Pipeline entry point.

python -m olist_lakehouse.pipeline                 # bronze -> silver -> gold -> quality
python -m olist_lakehouse.pipeline --layer silver  # one layer only
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from olist_lakehouse import bronze, gold, quality, silver
from olist_lakehouse.config import get_settings
from olist_lakehouse.spark import TableStore, get_spark

STEPS = ("bronze", "silver", "gold", "quality")
log = logging.getLogger("olist_lakehouse")


def run(steps: list[str], raw_path: str | None = None) -> dict:
    settings = get_settings()
    spark = get_spark()
    store = TableStore(spark, settings)
    store.ensure_schemas()
    summary: dict = {}

    for step in steps:
        start = time.perf_counter()
        log.info("=== %s ===", step.upper())
        if step == "bronze":
            summary[step] = bronze.run(spark, store, raw_path or settings.raw_path, bronze.new_batch_id())
        elif step == "silver":
            summary[step] = silver.run(store)
        elif step == "gold":
            summary[step] = gold.run(store)
        elif step == "quality":
            results = quality.run(store)
            summary[step] = {"checks": len(results), "failed": sum(not r.passed for r in results)}
        log.info("%s finished in %.1fs", step, time.perf_counter() - start)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Olist e-commerce lakehouse pipeline")
    parser.add_argument("--layer", choices=[*STEPS, "all"], default="all")
    parser.add_argument("--raw-path", help="override LAKEHOUSE_RAW_PATH")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    steps = list(STEPS) if args.layer == "all" else [args.layer]
    try:
        summary = run(steps, args.raw_path)
    except quality.DataQualityError as exc:
        log.error(str(exc))
        return 1
    for step, info in summary.items():
        log.info("%s: %s", step, info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
