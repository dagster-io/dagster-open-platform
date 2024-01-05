import pytest


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DBT_STITCH_DATABASE", "test")
    monkeypatch.setenv("DBT_STITCH_SCHEMA", "public")
