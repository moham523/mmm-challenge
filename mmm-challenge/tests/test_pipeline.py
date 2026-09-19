"""Tests for the MMM input mart pipeline. Run with: `poetry run pytest`"""

import csv
import sqlite3

import pytest

from mmm_challenge.defs.assets import _run_dbt
from mmm_challenge.ingest import DATA_RAW_DIR, DB_PATH, ingest_all


def test_ingest_all_row_counts(tmp_path):
    """ingest_all() should load the exact row counts from the real CSVs, and
    customer_id should still be present at this layer -- PII removal happens
    in dbt staging, not here."""
    db_path = tmp_path / "test.db"

    row_counts = ingest_all(data_dir=DATA_RAW_DIR, db_path=db_path)

    assert row_counts == {"raw_spend": 32, "raw_transactions": 56, "raw_cpi": 3}

    conn = sqlite3.connect(db_path)
    try:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(raw_transactions)")]
    finally:
        conn.close()
    assert "customer_id" in columns


def test_mmm_mart_output_correctness():
    """Integration test: runs the full pipeline (ingest + dbt) against the
    real data and checks the actual mmm_mart output, not just that it ran
    without error. Expected values are computed from the raw CSVs here
    rather than hardcoded, so the test stays correct if the sample data
    ever changes."""
    ingest_all()
    _run_dbt(["run"])

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM mmm_mart")
        column_names = [column[0] for column in cur.description]
        rows = cur.fetchall()
    finally:
        conn.close()

    assert len(rows) == 8
    assert "customer_id" not in column_names

    mart_by_week = {row[column_names.index("week_end_date")]: dict(zip(column_names, row)) for row in rows}

    # Revenue for the week ending 2026-01-11 should equal the hand-summed
    # transactions from 2026-01-05 through 2026-01-11 in the raw CSV.
    with (DATA_RAW_DIR / "transactions.csv").open(newline="") as f:
        expected_revenue = sum(
            float(row["revenue"]) for row in csv.DictReader(f) if "2026-01-05" <= row["date"] <= "2026-01-11"
        )
    assert mart_by_week["2026-01-11"]["revenue"] == pytest.approx(expected_revenue)

    # Every January week should carry January's CPI value from the raw CSV.
    with (DATA_RAW_DIR / "cpi.csv").open(newline="") as f:
        expected_january_cpi = next(
            float(row["cpi_value"]) for row in csv.DictReader(f) if row["month"] == "2026-01"
        )
    for week in ("2026-01-11", "2026-01-18", "2026-01-25"):
        assert mart_by_week[week]["cpi_value"] == pytest.approx(expected_january_cpi)

    # All 4 channel/subchannel spend columns should be present and populated.
    spend_columns = [
        "spend_paid_search_google",
        "spend_paid_search_bing",
        "spend_social_facebook",
        "spend_social_instagram",
    ]
    for column in spend_columns:
        assert column in column_names
    for week_row in mart_by_week.values():
        for column in spend_columns:
            assert week_row[column] is not None
