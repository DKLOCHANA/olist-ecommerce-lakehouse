.PHONY: install sample data pipeline analytics test lint format clean deploy

install:            ## install the package with dev tools
	pip install -e ".[dev]"

sample:             ## generate the small synthetic sample dataset
	python scripts/generate_sample_data.py --out data/raw --orders 2000

data:               ## download the real Olist dataset from Kaggle
	bash scripts/download_olist.sh data/raw

pipeline:           ## bronze -> silver -> gold -> data quality
	python -m olist_lakehouse.pipeline

analytics:          ## run the business SQL questions on gold
	python -m olist_lakehouse.analytics --export docs/sample_results.md

test:
	pytest

lint:
	ruff check . && ruff format --check .

format:
	ruff format . && ruff check . --fix

clean:
	rm -rf data/lakehouse spark-warehouse metastore_db .pytest_cache

deploy:             ## deploy + run the Databricks job (needs databricks CLI auth)
	databricks bundle deploy -t dev && databricks bundle run olist_lakehouse_job -t dev
