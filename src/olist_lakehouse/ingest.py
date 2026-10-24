"""Ingest: copy the raw Olist files from the source system into the lakehouse landing zone.

Two sources are supported:

* ``adls``   - Azure Data Lake Storage Gen2 (the production path). Reads the newest dated
               folder under ``<container>/<prefix>/`` (e.g. ``raw/olist/2026-10-24/``),
               authenticating with a read-only SAS token. ``.csv.gz`` files are decompressed.
* ``kaggle`` - downloads the public dataset zip straight from Kaggle (handy for a first run
               or a demo without Azure).

The landing zone is a folder: a Unity Catalog Volume on Databricks
(``/Volumes/workspace/raw/landing``) or ``data/raw`` locally. Bronze reads from there.

    python -m olist_lakehouse.ingest --source adls --account olistlakelochana --target /Volumes/...
    python -m olist_lakehouse.ingest --source kaggle --target data/raw
"""

from __future__ import annotations

import argparse
import gzip
import io
import logging
import os
import sys
import urllib.request
import zipfile
from pathlib import Path

from olist_lakehouse.config import get_settings
from olist_lakehouse.sources import SOURCES

log = logging.getLogger(__name__)

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
WANTED = {s.file for s in SOURCES.values()}
SECRET_SCOPE, SECRET_KEY = "olist", "adls-sas"


# --------------------------------------------------------------------------- helpers
def _write(target: Path, name: str, data: bytes) -> None:
    if name.endswith(".gz"):
        data, name = gzip.decompress(data), name[:-3]
    if name not in WANTED:
        return
    target.mkdir(parents=True, exist_ok=True)
    (target / name).write_bytes(data)
    log.info("landed %s (%.1f MB)", name, len(data) / 1e6)


def _check_complete(target: Path) -> list[str]:
    missing = sorted(WANTED - {p.name for p in target.glob("*.csv")})
    if missing:
        raise RuntimeError(f"Landing zone is missing files: {missing}")
    return sorted(WANTED)


def get_sas_token() -> str:
    """SAS token from the environment, or from the Databricks secret scope ``olist/adls-sas``."""
    token = os.getenv("AZURE_STORAGE_SAS")
    if token:
        return token.lstrip("?")
    try:
        from databricks.sdk.runtime import dbutils  # available on Databricks

        return dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY).lstrip("?")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "No Azure credential found. Set AZURE_STORAGE_SAS, or store a read-only SAS token in "
            f"the Databricks secret scope '{SECRET_SCOPE}' (key '{SECRET_KEY}') - see "
            "notebooks/00_connect_azure.py"
        ) from exc


# --------------------------------------------------------------------------- sources
def from_adls(account: str, container: str, prefix: str, target: Path, folder: str | None = None) -> str:
    from azure.storage.filedatalake import DataLakeServiceClient

    service = DataLakeServiceClient(f"https://{account}.dfs.core.windows.net", credential=get_sas_token())
    fs = service.get_file_system_client(container)

    if folder is None:  # newest dated load folder, e.g. olist/2026-10-24
        folders = sorted(p.name for p in fs.get_paths(path=prefix, recursive=False) if p.is_directory)
        if not folders:
            raise RuntimeError(f"No load folders found under {container}/{prefix}")
        folder = folders[-1]
    log.info("ingesting from abfss://%s@%s.dfs.core.windows.net/%s", container, account, folder)

    for path in fs.get_paths(path=folder, recursive=False):
        if path.is_directory:
            continue
        name = path.name.rsplit("/", 1)[-1]
        _write(target, name, fs.get_file_client(path.name).download_file().readall())
    return folder


def from_kaggle(target: Path) -> str:
    log.info("downloading %s", KAGGLE_URL)
    with urllib.request.urlopen(KAGGLE_URL, timeout=300) as resp:
        archive = zipfile.ZipFile(io.BytesIO(resp.read()))
    for name in archive.namelist():
        _write(target, name, archive.read(name))
    return KAGGLE_URL


def run(
    source: str,
    target: str,
    account: str | None = None,
    container: str = "raw",
    prefix: str = "olist",
    folder: str | None = None,
) -> dict:
    target_path = Path(target)
    if target_path.exists():  # landing zone holds only the current load
        for f in target_path.glob("*.csv"):
            f.unlink()
    if source == "adls":
        if not account:
            raise ValueError("--account is required for --source adls")
        origin = from_adls(account, container, prefix, target_path, folder)
    elif source == "kaggle":
        origin = from_kaggle(target_path)
    else:
        raise ValueError(f"unknown source {source!r}")
    files = _check_complete(target_path)
    log.info("ingest complete: %s files from %s", len(files), origin)
    return {"source": source, "origin": origin, "files": len(files)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Copy raw Olist files into the landing zone")
    p.add_argument("--source", choices=["adls", "kaggle"], default="adls")
    p.add_argument("--target", default=None, help="landing folder (default: LAKEHOUSE_RAW_PATH)")
    p.add_argument("--account", default=os.getenv("AZURE_STORAGE_ACCOUNT"))
    p.add_argument("--container", default="raw")
    p.add_argument("--prefix", default="olist")
    p.add_argument("--folder", default=None, help="specific load folder; default = newest")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    run(a.source, a.target or get_settings().raw_path, a.account, a.container, a.prefix, a.folder)
    return 0


if __name__ == "__main__":
    sys.exit(main())
