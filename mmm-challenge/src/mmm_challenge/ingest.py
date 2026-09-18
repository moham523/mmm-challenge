"""CSV ingest logic: load raw source files into SQLite.

Candidate: implement this module (reads from ../../data/raw/*.csv, writes
tables into the SQLite file your dbt profile points at -- Python's built-in
`sqlite3` module is sufficient, no extra dependency needed). Import it from
your Dagster assets with `from mmm_challenge.ingest import ingest_all`.
"""


def ingest_all() -> None:
    """Ingest spend.csv, transactions.csv, and cpi.csv into SQLite."""
    raise NotImplementedError


if __name__ == "__main__":
    ingest_all()
