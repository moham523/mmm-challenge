# MMM Input Mart Coding Challenge

## Setup

**Prerequisites:** Python 3.10–3.13 and Poetry installed (dbt doesn't support 3.14 yet, see `INSTRUCTIONS.md` for full detail).

**Install:** from this directory (the Poetry project root, note that `data/` lives one level up as a sibling of this folder, not nested inside it):

```bash
poetry install
```

**Verify (optional):**

```bash
poetry run dg --version
```

## How to Run

**Option A: dbt CLI only** (fastest for iterating on models, no Dagster involved):

```bash
poetry run python -m mmm_challenge.ingest   # load CSVs into SQLite
cd dbt
poetry run dbt run                           # build all models
poetry run dbt test                          # run all dbt tests
```

**Option B: full pipeline via Dagster UI** (the intended orchestrated path, ingest → staging → intermediate → marts → tests → CSV export, each as its own visible stage):

```bash
poetry run dg dev
```

Open `localhost:3000`, go to the Assets tab, and click "Materialize all." Each of the 6 stages (`raw_source_tables`, `dbt_staging`, `dbt_intermediate`, `mmm_mart`, the `mmm_mart_tests` check, `mmm_mart_csv`) runs in sequence. This is also how the weekly schedule runs it automatically (Mondays 6am), a manual "Materialize all" triggers the same thing on demand.

**Option C: Dagster CLI, no UI** (same as B, scriptable/headless):

```bash
poetry run dg launch --assets "*"
```

## Assumptions

- Raw tables are dropped and fully reloaded on every run rather than preserved or appended to. I wasn't sure if previous raw data was required to be saved, so I chose the simpler approach of dropping each table and recreating it from the latest CSV.
- Every month in the mart's date range is assumed to have a corresponding CPI row in the raw data. Missing months aren't handled.

## Testing

Automated: `dbt test` (17 tests, schema tests plus singular SQL tests) runs on every pipeline execution as a blocking check before the CSV export, including the weekly schedule.

Manual: `pytest` (in `tests/test_pipeline.py`) covers ingestion row counts and is run on demand, not part of the automated schedule.
