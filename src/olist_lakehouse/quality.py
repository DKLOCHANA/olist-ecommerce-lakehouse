"""Data quality checks on silver and gold tables.

Row-level problems are already quarantined in silver; these are table-level
checks (uniqueness, referential integrity, reconciliation, thresholds). Every
result is appended to ``gold.dq_results`` so quality can be tracked over time.
A failing ``error`` check stops the pipeline; a failing ``warn`` check is logged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F

from olist_lakehouse.sources import ORDER_STATUSES, SOURCES
from olist_lakehouse.spark import TableStore

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    check: str
    table: str
    severity: str  # "error" | "warn"
    passed: bool
    failed_rows: int
    details: str = ""


class DataQualityError(RuntimeError):
    pass


class Checks:
    def __init__(self, store: TableStore):
        self.store = store
        self.results: list[CheckResult] = []

    def _add(self, check, table, severity, failed_rows, details=""):
        result = CheckResult(check, table, severity, failed_rows == 0, int(failed_rows), details)
        self.results.append(result)
        level = logging.INFO if result.passed else logging.WARNING
        log.log(level, "[%s] %s on %s: %s failing rows %s", severity, check, table, failed_rows, details)

    def row_count_positive(self, layer, table, severity="error"):
        n = self.store.read(layer, table).count()
        self._add("row_count_positive", f"{layer}.{table}", severity, 0 if n > 0 else 1, f"rows={n}")

    def unique(self, layer, table, keys, severity="error"):
        dupes = self.store.read(layer, table).groupBy(*keys).count().filter("count > 1").count()
        self._add(f"unique({', '.join(keys)})", f"{layer}.{table}", severity, dupes)

    def not_null(self, layer, table, column, severity="error"):
        n = self.store.read(layer, table).filter(F.col(column).isNull()).count()
        self._add(f"not_null({column})", f"{layer}.{table}", severity, n)

    def accepted_values(self, layer, table, column, values, severity="error"):
        n = self.store.read(layer, table).filter(~F.col(column).isin(*values)).count()
        self._add(f"accepted_values({column})", f"{layer}.{table}", severity, n)

    def min_value(self, layer, table, column, minimum, severity="error"):
        n = self.store.read(layer, table).filter(F.col(column) < minimum).count()
        self._add(f"{column} >= {minimum}", f"{layer}.{table}", severity, n)

    def relationship(self, child, column, parent, parent_column, severity="error"):
        c = self.store.read("gold", child)
        p = self.store.read("gold", parent).select(F.col(parent_column).alias("_pk"))
        orphans = c.join(p, c[column] == p["_pk"], "left_anti").count()
        self._add(f"relationship({column} -> {parent}.{parent_column})", f"gold.{child}", severity, orphans)

    def unknown_member_ratio(self, table, column, max_ratio, severity="warn"):
        df = self.store.read("gold", table)
        total = df.count()
        unknown = df.filter(F.col(column) == -1).count()
        ratio = unknown / total if total else 0
        self._add(
            f"unknown_{column}_ratio <= {max_ratio:.0%}",
            f"gold.{table}",
            severity,
            unknown if ratio > max_ratio else 0,
            f"ratio={ratio:.2%}",
        )

    def quarantine_ratio(self, table, max_ratio, severity="warn"):
        """Share of the latest batch that was quarantined in silver."""
        valid = self.store.read("silver", table).count()
        quarantine = self.store.read("silver", f"{table}_quarantine")
        last_batch = self.store.read("bronze", table).agg(F.max("_batch_id")).first()[0]
        quarantined = quarantine.filter(F.col("_batch_id") == last_batch).count()
        ratio = quarantined / (valid + quarantined) if (valid + quarantined) else 0
        self._add(
            f"quarantine_ratio <= {max_ratio:.0%}",
            f"silver.{table}",
            severity,
            quarantined if ratio > max_ratio else 0,
            f"ratio={ratio:.2%}",
        )

    def reconcile_counts(self, severity="error"):
        """Every valid silver order item with a valid order must reach the fact table."""
        items = self.store.read("silver", "order_items")
        orders = self.store.read("silver", "orders").select("order_id")
        expected = items.join(orders, "order_id", "left_semi").count()
        actual = self.store.read("gold", "fact_order_items").count()
        self._add(
            "reconcile(silver.order_items -> fact_order_items)",
            "gold.fact_order_items",
            severity,
            abs(expected - actual),
            f"expected={expected} actual={actual}",
        )

    def reconcile_revenue(self, severity="error"):
        silver = self.store.read("silver", "order_payments").join(
            self.store.read("silver", "orders").select("order_id"), "order_id", "left_semi"
        )
        expected = silver.agg(F.sum("payment_value")).first()[0] or 0
        actual = self.store.read("gold", "fact_payments").agg(F.sum("payment_value")).first()[0] or 0
        self._add(
            "reconcile(payment_value)",
            "gold.fact_payments",
            severity,
            0 if expected == actual else 1,
            f"expected={expected} actual={actual}",
        )


def run(store: TableStore) -> list[CheckResult]:
    c = Checks(store)

    for source in SOURCES.values():
        c.row_count_positive("silver", source.name)
        c.unique("silver", source.name, list(source.keys))
        c.quarantine_ratio(source.name, max_ratio=0.05)

    dims = {
        "dim_customer": "customer_key",
        "dim_product": "product_key",
        "dim_seller": "seller_key",
        "dim_date": "date_key",
    }
    for dim, key in dims.items():
        c.row_count_positive("gold", dim)
        c.unique("gold", dim, [key])
        c.not_null("gold", dim, key)

    c.unique("gold", "fact_order_items", ["order_id", "order_item_id"])
    c.unique("gold", "fact_payments", ["order_id", "payment_sequential"])
    for dim, key in dims.items():
        fk = "purchase_date_key" if dim == "dim_date" else key
        c.relationship("fact_order_items", fk, dim, key)
    c.relationship("fact_payments", "customer_key", "dim_customer", "customer_key")
    c.accepted_values("gold", "fact_order_items", "order_status", ORDER_STATUSES)
    c.min_value("gold", "fact_order_items", "price", 0)
    c.min_value("gold", "fact_payments", "payment_value", 0)
    c.unknown_member_ratio("fact_order_items", "customer_key", max_ratio=0.05)
    c.unknown_member_ratio("fact_order_items", "product_key", max_ratio=0.05)
    c.reconcile_counts()
    c.reconcile_revenue()

    run_at = datetime.now(timezone.utc)
    rows = [Row(run_at=run_at, **r.__dict__) for r in c.results]
    store.write(store.spark.createDataFrame(rows), "gold", "dq_results", mode="append")

    errors = [r for r in c.results if not r.passed and r.severity == "error"]
    warnings = [r for r in c.results if not r.passed and r.severity == "warn"]
    log.info("data quality: %s checks, %s errors, %s warnings", len(c.results), len(errors), len(warnings))
    if errors:
        summary = "; ".join(f"{r.check} on {r.table} ({r.failed_rows})" for r in errors)
        raise DataQualityError(f"{len(errors)} data quality check(s) failed: {summary}")
    return c.results
