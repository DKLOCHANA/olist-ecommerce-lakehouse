from pathlib import Path

from olist_lakehouse.config import Settings
from olist_lakehouse.gold import GOLD_SQL_DIR, model_files, model_name, render


class _Store:
    def __init__(self, env):
        self.settings = Settings(env=env, raw_path="", lakehouse_path="", catalog="olist")

    def sql_name(self, layer, name):
        if self.settings.is_databricks:
            return f"olist.{layer}.{name}"
        return f"{layer}_{name}"


def test_render_local_and_databricks():
    sql = "SELECT * FROM {{ silver.orders }} JOIN {{gold.dim_date}}"
    assert render(sql, _Store("local")) == "SELECT * FROM silver_orders JOIN gold_dim_date"
    assert render(sql, _Store("databricks")) == "SELECT * FROM olist.silver.orders JOIN olist.gold.dim_date"


def test_models_are_ordered_dims_before_facts():
    names = [model_name(p) for p in model_files()]
    assert names.index("dim_customer") < names.index("fact_order_items")
    assert names.index("fact_order_items") < names.index("mart_daily_sales")


def test_model_name():
    assert model_name(Path("05_fact_order_items.sql")) == "fact_order_items"
    assert GOLD_SQL_DIR.exists()
