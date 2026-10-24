"""Upload the raw Olist CSV files to Azure Data Lake Storage Gen2 (the landing zone).

    pip install -e ".[azure]"
    az login                                   # or set AZURE_STORAGE_CONNECTION_STRING
    python scripts/upload_to_adls.py --account <storage-account> --container raw

Files land in  abfss://<container>@<account>.dfs.core.windows.net/olist/<yyyy-mm-dd>/
so every load is kept as its own dated folder.
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


def client(account: str) -> DataLakeServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn:
        return DataLakeServiceClient.from_connection_string(conn)
    url = f"https://{account}.dfs.core.windows.net"
    return DataLakeServiceClient(url, credential=DefaultAzureCredential())


def upload(account: str, container: str, source: Path, prefix: str) -> str:
    fs = client(account).get_file_system_client(container)
    if not fs.exists():
        fs.create_file_system()
    files = sorted(source.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in {source}")
    for f in files:
        fs.get_file_client(f"{prefix}/{f.name}").upload_data(f.read_bytes(), overwrite=True)
        print(f"uploaded {f.name} ({f.stat().st_size / 1e6:.1f} MB)")
    return f"abfss://{container}@{account}.dfs.core.windows.net/{prefix}"


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--account", required=True, help="storage account name (hierarchical namespace on)")
    p.add_argument("--container", default="raw")
    p.add_argument("--source", type=Path, default=Path("data/raw"))
    p.add_argument("--prefix", default=f"olist/{date.today().isoformat()}")
    a = p.parse_args()
    path = upload(a.account, a.container, a.source, a.prefix)
    print(f"\nDone. Point the pipeline at it with:\n  --raw-path {path}")
