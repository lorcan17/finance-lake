from embed_enrich.duckdb_conn import connect
from embed_enrich.normalise import ensure_schema


def test_ensure_schema_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("FINANCE_DUCKDB", str(tmp_path / "t.duckdb"))
    con = connect()
    ensure_schema(con)
    ensure_schema(con)  # second call must not error
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'"
    ).fetchall()
    names = {t[0] for t in tables}
    assert "dim_merchants" in names
    assert "merchant_review_queue" in names
    con.close()
