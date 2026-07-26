from windex import db
from windex.db import canonical


def test_init_db_returns_canonical_metadata(monkeypatch):
    expected = {
        "schema_generation": 2,
        "contract_epoch": 2,
        "bootstrap_id": "test-generation",
    }
    monkeypatch.setattr(
        canonical, "init_canonical_db", lambda _conn: expected)

    assert db.init_db(object()) is expected
