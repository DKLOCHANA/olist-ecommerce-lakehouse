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
    monkeypatch.setenv("AZURE_STORAGE_SAS", "?sv=2024-11-04&ss=b&sig=abc%3D")
    assert ingest.get_sas_token() == "sv=2024-11-04&ss=b&sig=abc%3D"


@pytest.mark.parametrize(
    "pasted",
    [
        "sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-01-01T00:00:00Z&sig=abc%3D",
        "?sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-01-01T00:00:00Z&sig=abc%3D\n",
        "BlobEndpoint=https://x.blob.core.windows.net/;SharedAccessSignature="
        "sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-01-01T00:00:00Z&sig=abc%3D",
        "https://x.blob.core.windows.net/?sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-01-01T00:00:00Z&sig=abc%3D",
        "some text pasted by accident put_secret(scope, key) sv=2024-11-04&ss=b&srt=co&sp=rl"
        "&se=2027-01-01T00:00:00Z&sig=abc%3D",
    ],
)
def test_clean_sas_handles_messy_pastes(pasted):
    assert ingest.clean_sas(pasted) == "sv=2024-11-04&ss=b&srt=co&sp=rl&se=2027-01-01T00:00:00Z&sig=abc%3D"


def test_clean_sas_rejects_garbage():
    with pytest.raises(ValueError):
        ingest.clean_sas("not a token")
