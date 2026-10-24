"""Databricks job entry point (used by databricks.yml).

Each job task runs one step:

    run_layer.py --src <bundle>/src --layer ingest --ingest-source adls --azure-account <name>
    run_layer.py --src <bundle>/src --layer bronze
"""

import argparse
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True, help="path to the bundle's src folder")
parser.add_argument(
    "--layer", required=True, choices=["ingest", "bronze", "silver", "gold", "quality", "all"]
)
parser.add_argument("--ingest-source", default="adls", choices=["adls", "kaggle"])
parser.add_argument("--azure-account", default=None)
parser.add_argument("--catalog", default="workspace")
parser.add_argument("--raw-path", default=None)
args, _ = parser.parse_known_args()

os.environ["LAKEHOUSE_ENV"] = "databricks"
os.environ["LAKEHOUSE_CATALOG"] = args.catalog
if args.raw_path:
    os.environ["LAKEHOUSE_RAW_PATH"] = args.raw_path
sys.path.insert(0, args.src)

if args.layer == "ingest":
    from pyspark.sql import SparkSession

    from olist_lakehouse.ingest import main  # noqa: E402

    spark = SparkSession.builder.getOrCreate()  # make sure the landing volume exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.raw")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {args.catalog}.raw.landing")

    ingest_args = ["--source", args.ingest_source]
    if args.azure_account:
        ingest_args += ["--account", args.azure_account]
    exit_code = main(ingest_args)
else:
    from olist_lakehouse.pipeline import main  # noqa: E402

    exit_code = main(["--layer", args.layer])
if exit_code != 0:
    raise SystemExit(exit_code)  # fail the task so the job run is marked failed
