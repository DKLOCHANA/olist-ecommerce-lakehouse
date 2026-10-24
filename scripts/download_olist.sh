#!/usr/bin/env bash
# Download the real Olist dataset (~45 MB zipped, ~100k orders) from Kaggle.
# Needs a free Kaggle account and an API token in ~/.kaggle/kaggle.json
# (Kaggle -> Settings -> API -> Create New Token).
set -euo pipefail
OUT="${1:-data/raw}"
pip install --quiet kaggle
mkdir -p "$OUT"
kaggle datasets download -d olistbr/brazilian-ecommerce -p "$OUT" --unzip
# not used by the pipeline (1M rows of zip-code coordinates)
rm -f "$OUT/olist_geolocation_dataset.csv"
ls -lh "$OUT"
