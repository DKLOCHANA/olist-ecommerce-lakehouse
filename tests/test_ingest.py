import gzip

import pytest

from olist_lakehouse import ingest


def test_write_decompresses_gz_and_skips_unknown_files(tmp_path):
    ingest._write(tmp_path, "olist_orders_dataset.csv.gz", gzip.compress(b"order_id\n1\n"))
    ingest._write(tmp_path, "olist_geolocation_dataset.csv", b"ignored")
    assert (tmp_path / "olist_orders_dataset.csv").read_bytes() == b"order_id\n1\n"
    assert not (tmp_path / "olist_geolocation_dataset.csv").exists()


def test_incomplete_landing_zone_fails(tmp_path):
    ingest._write(tmp_path, "olist_orders_dataset.csv", b"order_id\n")
    with pytest.raises(RuntimeError, match="missing files"):
        ingest._check_complete(tmp_path)


def test_sas_token_from_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_SAS", "?sv=2024&sig=abc")
    assert ingest.get_sas_token() == "sv=2024&sig=abc"
