"""Runtime configuration.

The same code runs in two modes, selected with the ``LAKEHOUSE_ENV``
environment variable:

* ``local``      - laptop / CI. Delta tables are stored as folders on disk.
* ``databricks`` - Databricks (Free Edition or Azure Databricks). Tables are
                   Unity Catalog managed tables: ``<catalog>.<layer>.<table>``
                   (catalog defaults to ``workspace``, the Free Edition default).

Raw files can come from a local folder, a Unity Catalog Volume or an Azure
Data Lake Storage Gen2 container (``abfss://...``) - set ``LAKEHOUSE_RAW_PATH``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    env: str
    raw_path: str
    lakehouse_path: str
    catalog: str

    @property
    def is_databricks(self) -> bool:
        return self.env == "databricks"


def get_settings() -> Settings:
    env = os.getenv("LAKEHOUSE_ENV", "local").lower()
    if env not in {"local", "databricks"}:
        raise ValueError(f"LAKEHOUSE_ENV must be 'local' or 'databricks', got {env!r}")

    catalog = os.getenv("LAKEHOUSE_CATALOG", "workspace")
    local_raw = str(PROJECT_ROOT / "data" / "raw")
    default_raw = f"/Volumes/{catalog}/raw/landing" if env == "databricks" else local_raw
    return Settings(
        env=env,
        raw_path=os.getenv("LAKEHOUSE_RAW_PATH", default_raw),
        lakehouse_path=os.getenv("LAKEHOUSE_PATH", str(PROJECT_ROOT / "data" / "lakehouse")),
        catalog=catalog,
    )
