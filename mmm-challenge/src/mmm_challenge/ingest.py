"""CSV ingest logic: load raw source files into SQLite.

Reads spend.csv, transactions.csv, and cpi.csv from data/raw/ and loads
each into its own raw table in SQLite. No filtering happens here (e.g.
customer_id is still present) -- dropping columns is dbt's job later.
"""

import csv
import sqlite3
from pathlib import Path
from typing import Callable

# Figure out folder paths relative to this file, so it works no matter
# where the script is run from.
# ingest.py -> mmm_challenge -> src -> mmm-challenge (project root) -> mmm-challenge-starter (repo root)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = Path(__file__).resolve().parents[3]

DATA_RAW_DIR = _REPO_ROOT / "data" / "raw"
DB_PATH = _PROJECT_ROOT / "dbt" / "mmm_challenge.db"

# Each column is (name, SQLite type, function to convert the CSV string).
ColumnSpec = tuple[str, str, Callable[[str], object]]

# Maps each CSV file to the table it loads into and its columns.
TABLE_SPECS: dict[str, tuple[str, list[ColumnSpec]]] = {
    "spend.csv": (
        "raw_spend",
        [
            ("week_end_date", "TEXT", str),
            ("channel", "TEXT", str),
            ("subchannel", "TEXT", str),
            ("spend", "REAL", float),
            ("impressions", "INTEGER", int),
        ],
    ),
    "transactions.csv": (
        "raw_transactions",
        [
            ("date", "TEXT", str),
            ("customer_id", "TEXT", str),
            ("revenue", "REAL", float),
            ("units", "INTEGER", int),
        ],
    ),
    "cpi.csv": (
        "raw_cpi",
        [
            ("month", "TEXT", str),
            ("cpi_value", "REAL", float),
        ],
    ),
}


def _read_csv_rows(csv_path: Path, columns: list[ColumnSpec]) -> list[tuple[object, ...]]:
    """Read a CSV file and convert each row into a tuple, in column order."""
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        return [tuple(convert(row[name]) for name, _, convert in columns) for row in reader]


def _load_table(conn: sqlite3.Connection, table_name: str, columns: list[ColumnSpec], rows: list[tuple[object, ...]]) -> None:
    """Replace a table with fresh data: drop it, recreate it, insert all rows."""
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    column_defs = ", ".join(f"{name} {sql_type}" for name, sql_type, _ in columns)
    cur.execute(f"CREATE TABLE {table_name} ({column_defs})")

    placeholders = ", ".join("?" for _ in columns)
    cur.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
    conn.commit()


def _validate_load(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[ColumnSpec],
    expected_rows: list[tuple[object, ...]],
) -> None:
    """Check the load actually worked: right row count, and the first
    few rows' values match the CSV exactly (catches silent corruption)."""
    cur = conn.cursor()

    # 1. Row count must match exactly -- nothing dropped, nothing duplicated.
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual_count = cur.fetchone()[0]
    if actual_count != len(expected_rows):
        raise ValueError(f"{table_name}: expected {len(expected_rows)} rows, found {actual_count}")

    # 2. Spot-check up to the first 10 rows' content (fewer if the CSV is smaller).
    sample_size = min(10, len(expected_rows))
    if sample_size == 0:
        return

    col_names = ", ".join(name for name, _, _ in columns)
    cur.execute(f"SELECT {col_names} FROM {table_name} ORDER BY rowid LIMIT ?", (sample_size,))
    actual_sample = cur.fetchall()

    for row_index, (expected, actual) in enumerate(zip(expected_rows[:sample_size], actual_sample)):
        for col_index, (name, _, _) in enumerate(columns):
            if expected[col_index] != actual[col_index]:
                raise ValueError(
                    f"{table_name}: row {row_index} column '{name}' mismatch "
                    f"- csv had {expected[col_index]!r}, db has {actual[col_index]!r}"
                )


def ingest_all(data_dir: Path = DATA_RAW_DIR, db_path: Path = DB_PATH) -> dict[str, int]:
    """Ingest spend.csv, transactions.csv, and cpi.csv into SQLite.

    Returns a dict of table name -> number of rows loaded.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        row_counts = {}
        for csv_filename, (table_name, columns) in TABLE_SPECS.items():
            rows = _read_csv_rows(data_dir / csv_filename, columns)
            _load_table(conn, table_name, columns, rows)
            _validate_load(conn, table_name, columns, rows)
            row_counts[table_name] = len(rows)
        return row_counts
    finally:
        conn.close()


if __name__ == "__main__":
    for table_name, row_count in ingest_all().items():
        print(f"{table_name}: {row_count} rows")
