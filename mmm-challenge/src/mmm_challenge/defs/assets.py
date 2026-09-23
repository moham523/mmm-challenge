"""Dagster asset graph: ingest -> dbt staging -> intermediate -> marts -> test -> export CSV."""

import csv
import sqlite3
import subprocess

from dagster import AssetCheckResult, Failure, MaterializeResult, MetadataValue, asset, asset_check

from mmm_challenge.ingest import DATA_RAW_DIR, DB_PATH, ingest_all

# Reuse ingest.py's paths instead of recomputing them.
DBT_PROJECT_DIR = DB_PATH.parent
MART_CSV_PATH = DATA_RAW_DIR.parent / "output" / "mmm_mart.csv"


def _run_dbt(args: list[str]) -> str:
    """Run a dbt command as a subprocess; raises Failure with stdout/stderr on error."""
    # cwd pinned to DBT_PROJECT_DIR -- profiles.yml resolves paths relative to cwd, not --project-dir.
    result = subprocess.run(
        ["dbt", *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Failure(
            description=f"dbt {' '.join(args)} failed",
            metadata={"stdout": result.stdout, "stderr": result.stderr},
        )
    return result.stdout


@asset
def raw_source_tables() -> MaterializeResult:
    """Load spend.csv, transactions.csv, and cpi.csv into SQLite raw tables."""
    row_counts = ingest_all() 
    return MaterializeResult(metadata={name: MetadataValue.int(count) for name, count in row_counts.items()})


@asset(deps=[raw_source_tables])
def dbt_staging() -> None:
    """Build the staging dbt models."""
    _run_dbt(["run", "--select", "staging"])


@asset(deps=[dbt_staging])
def dbt_intermediate() -> None:
    """Build the intermediate dbt models."""
    _run_dbt(["run", "--select", "intermediate"])


@asset(deps=[dbt_intermediate])
def mmm_mart() -> None:
    """Build the final mmm_mart table."""
    _run_dbt(["run", "--select", "marts"])


@asset_check(asset=mmm_mart, blocking=True)
def mmm_mart_tests() -> AssetCheckResult:
    """Run `dbt test`; blocking=True skips mmm_mart_csv if any test fails."""
    _run_dbt(["test"])
    return AssetCheckResult(passed=True)


@asset(deps=[mmm_mart])
def mmm_mart_csv() -> MaterializeResult:
    """Export mmm_mart to data/output/mmm_mart.csv, overwriting cleanly."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM mmm_mart")
        column_names = [column[0] for column in cur.description]
        rows = cur.fetchall()
    finally:
        conn.close()

    MART_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MART_CSV_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerows(rows)

    return MaterializeResult(metadata={"rows_exported": MetadataValue.int(len(rows))})
