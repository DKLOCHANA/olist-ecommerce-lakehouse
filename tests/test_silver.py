from datetime import datetime

from olist_lakehouse.silver import transform
from olist_lakehouse.sources import SOURCES

TS = datetime(2024, 1, 1)


def _bronze(spark, source, rows, batch="b1", ts=TS):
    schema = ", ".join(f"`{c}` string" for c in source.columns)
    data = [(*r, ts, batch) for r in rows]
    return spark.createDataFrame(data, f"{schema}, _ingested_at timestamp, _batch_id string")


def test_customers_are_trimmed_standardised_and_deduplicated(spark):
    src = SOURCES["customers"]
    rows = [
        ("c1", "u1", "01001", "  SAO PAULO ", " sp "),
        ("c1", "u1", "01001", "sao paulo", "SP"),  # duplicate key
        ("c2", "u2", "20000", "rio de janeiro", "RJ"),
    ]
    valid, invalid = transform(_bronze(spark, src, rows), src)
    out = {r.customer_id: r for r in valid.collect()}
    assert set(out) == {"c1", "c2"}
    assert out["c1"].customer_state == "SP"
    assert out["c1"].customer_city == "Sao Paulo"
    assert invalid.count() == 0


def test_dedup_keeps_latest_ingested_row(spark):
    src = SOURCES["sellers"]
    old = _bronze(spark, src, [("s1", "1000", "old city", "SP")], ts=datetime(2024, 1, 1))
    new = _bronze(spark, src, [("s1", "1000", "new city", "SP")], ts=datetime(2024, 2, 1))
    valid, _ = transform(old.unionByName(new), src)
    assert valid.collect()[0].seller_city == "New City"


def test_invalid_rows_are_quarantined_with_reasons(spark):
    src = SOURCES["order_items"]
    rows = [
        ("o1", "1", "p1", "s1", "2018-01-01 10:00:00", "10.50", "5.00"),
        ("o2", "1", "p1", "s1", "2018-01-01 10:00:00", "-1.00", "5.00"),
        ("o3", "1", "p1", "s1", "2018-01-01 10:00:00", "abc", "5.00"),
        (None, "1", "p1", "s1", "2018-01-01 10:00:00", "3.00", "5.00"),
    ]
    valid, invalid = transform(_bronze(spark, src, rows), src)
    assert [r.order_id for r in valid.collect()] == ["o1"]
    reasons = {r.order_id: set(r._dq_failures) for r in invalid.collect()}
    assert reasons["o2"] == {"price_non_negative"}
    assert reasons["o3"] == {"types_valid", "price_non_negative"}
    assert "order_id_not_null" in reasons[None]


def test_order_status_rules(spark):
    src = SOURCES["orders"]
    base = ("2018-01-01 10:00:00", None, None, None, "2018-01-20 00:00:00")
    rows = [
        ("o1", "c1", " Delivered ", *base),
        ("o2", "c1", "lost", *base),
    ]
    valid, invalid = transform(_bronze(spark, src, rows), src)
    assert valid.collect()[0].order_status == "delivered"
    assert invalid.collect()[0]._dq_failures == ["status_accepted"]


def test_types_are_cast(spark):
    src = SOURCES["order_payments"]
    valid, _ = transform(_bronze(spark, src, [("o1", "1", "boleto", "1", "99.90")]), src)
    types = dict(valid.dtypes)
    assert types["payment_sequential"] == "int"
    assert types["payment_value"] == "decimal(10,2)"
