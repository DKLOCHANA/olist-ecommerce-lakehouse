"""SparkSession factory and table I/O that works locally and on Databricks."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from olist_lakehouse.config import Settings, get_settings

LAYERS = ("bronze", "silver", "gold")


def get_spark(app_name: str = "olist-lakehouse") -> SparkSession:
    """Return the active Databricks session, or build a local Delta-enabled one."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    settings = get_settings()
    if settings.is_databricks:
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class TableStore:
    """Reads and writes Delta tables for a medallion layer.

    Locally a table is a Delta folder ``<lakehouse_path>/<layer>/<name>`` that is
    also registered as a temp view ``<layer>_<name>`` so SQL files can query it.
    On Databricks it is the Unity Catalog table ``<catalog>.<layer>.<name>``.
    """

    def __init__(self, spark: SparkSession, settings: Settings | None = None):
        self.spark = spark
        self.settings = settings or get_settings()

    def _check_layer(self, layer: str) -> None:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")

    def ensure_schemas(self) -> None:
        """On Databricks, create the bronze/silver/gold schemas if they don't exist."""
        if self.settings.is_databricks:
            for layer in LAYERS:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.settings.catalog}.{layer}")

    def location(self, layer: str, name: str) -> str:
        self._check_layer(layer)
        if self.settings.is_databricks:
            return f"{self.settings.catalog}.{layer}.{name}"
        return f"{self.settings.lakehouse_path}/{layer}/{name}"

    def write(
        self,
        df: DataFrame,
        layer: str,
        name: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
    ) -> None:
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        target = self.location(layer, name)
        if self.settings.is_databricks:
            writer.saveAsTable(target)
        else:
            writer.save(target)
            self.read(layer, name)  # refresh temp view

    def read(self, layer: str, name: str) -> DataFrame:
        target = self.location(layer, name)
        if self.settings.is_databricks:
            return self.spark.table(target)
        df = self.spark.read.format("delta").load(target)
        df.createOrReplaceTempView(f"{layer}_{name}")
        return df

    def exists(self, layer: str, name: str) -> bool:
        target = self.location(layer, name)
        if self.settings.is_databricks:
            return self.spark.catalog.tableExists(target)
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(self.spark, target)

    def upsert(self, df: DataFrame, layer: str, name: str, keys: tuple[str, ...]) -> None:
        """Idempotent MERGE on business keys (insert new rows, update changed ones)."""
        if not self.exists(layer, name):
            self.write(df, layer, name)
            return
        target = self.location(layer, name)
        target_sql = target if self.settings.is_databricks else f"delta.`{target}`"
        view = f"_upsert_{layer}_{name}"
        df.createOrReplaceTempView(view)
        on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        self.spark.sql(
            f"MERGE INTO {target_sql} t USING {view} s ON {on} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
        self.read(layer, name)

    def sql_name(self, layer: str, name: str) -> str:
        """Name to use for this table inside a SQL query."""
        if self.settings.is_databricks:
            return self.location(layer, name)
        return f"{layer}_{name}"
