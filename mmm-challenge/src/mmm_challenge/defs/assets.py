"""Dagster asset graph: ingest CSVs -> dbt build -> dbt test -> export mart CSV."""

import csv
import sqlite3
import subprocess

from dagster import AssetCheckResult, Failure, MaterializeResult, MetadataValue, asset, asset_check

from mmm_challenge.ingest import DATA_RAW_DIR, DB_PATH, ingest_all

# Reuse ingest.py's already-computed paths instead of recalculating parents[]
# ourselves -- DB_PATH lives inside dbt/, and data/raw's parent is data/.
DBT_PROJECT_DIR = DB_PATH.parent
MART_CSV_PATH = DATA_RAW_DIR.parent / "output" / "mmm_mart.csv"


def _run_dbt(args: list[str]) -> str:
    """Run a dbt command (e.g. ["run"] or ["test"]) as a subprocess, since this
    project doesn't have the dagster-dbt integration installed. Raises a
    Dagster Failure with dbt's captured stdout/stderr if the command fails.

    cwd is pinned to DBT_PROJECT_DIR -- profiles.yml's schema_directory: '.'
    resolves relative to the process's cwd, not to --project-dir, so without
    this dbt silently opens/creates the SQLite file in the wrong place
    whenever this runs from somewhere other than dbt/ (e.g. under Dagster).
    """
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
    """Load spend.csv, transactions.csv, and cpi.csv into SQLite raw tables.

    Deliberately no try/except around ingest_all(): if it raises, Dagster
    marks this step failed and skips downstream assets in this run, instead
    of silently continuing on to dbt with incomplete/stale raw tables.
    """
    row_counts = ingest_all()
    return MaterializeResult(metadata={name: MetadataValue.int(count) for name, count in row_counts.items()})


@asset(deps=[raw_source_tables])
def mmm_mart() -> None:
    """Build all dbt models (staging -> intermediate -> marts) via `dbt run`.
    Depends on raw_source_tables so ingestion always runs first."""
    _run_dbt(["run"])


@asset_check(asset=mmm_mart, blocking=True)
def mmm_mart_tests() -> AssetCheckResult:
    """Run `dbt test` (schema tests + the dropped-week singular test).
    blocking=True: if any test fails, mmm_mart_csv (downstream, same run)
    does not execute -- we don't want to export a CSV built on data that
    failed validation."""
    _run_dbt(["test"])
    return AssetCheckResult(passed=True)


@asset(deps=[mmm_mart])
def mmm_mart_csv() -> MaterializeResult:
    """Export the finished mmm_mart table to data/output/mmm_mart.csv, the
    actual deliverable file. Opened in "w" (truncate) mode so every run fully
    replaces the file instead of appending stale rows on top of old ones."""
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
