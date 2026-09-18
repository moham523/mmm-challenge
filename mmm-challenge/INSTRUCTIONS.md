# MMM Input Mart Coding Challenge

**Time Estimate**: 1-2 hours | **Tools**: Python, dbt, Dagster, SQLite

## Context

You're a Data Engineer on the Marketing Mix Modeling (MMM) team. The data science team depends on a weekly data mart that combines marketing spend, transaction outcomes, and macroeconomic data to power MMM models. Right now, this is built manually—your job is to automate it.

This challenge mirrors the real work you'd do on the MMM team: building robust, well-tested data pipelines that integrate multiple sources and produce clean, reliable inputs for downstream data science work.


## The Challenge

You'll receive three CSV files via email:
1. **spend.csv** – Weekly marketing spend by channel/subchannel
2. **transactions.csv** – Daily order-level transactions (includes customer ID)
3. **cpi.csv** – Monthly inflation data

**Your job**: Build a Python + dbt + Dagster pipeline that ingests these files into SQLite, performs light transformations, and outputs a weekly **MMM input mart** ready for data science teams.

### Requirements

**Mart Output**
- **Granularity**: One row per week
- **Columns**:
  - `week_end_date`: date for the end of the week (see note below)
  - Revenue: sum of transaction amounts
  - Marketing spend: one column per channel/subchannel combo
  - Monthly CPI: assign the monthly value to each week in that month

**Constraints**
- You may notice some columns in the source data that don't belong in the final mart. Consider which transformation layer is the right place to drop them, and why (hint: think about PII, data minimization, and why staging/intermediate/mart layering exists)
- Weeks are defined as Monday through Sunday. SQLite doesn't have a `date_trunc` function, but `date(<date_column>, 'weekday 0')` returns the next Sunday on/after a given date -- which is exactly the Monday-Sunday week's end date
- Mart should regenerate weekly when all three data sources are available
- Code must be version-controlled (Git repo with meaningful commits)
- Include a README documenting setup, how to run the pipeline, and any assumptions you made
- At least one test (data validation, schema check, or output correctness)

### Tech Stack

These tools run locally and mirror our production setup. Here's a quick primer in case they're new to you:

- **Python**: General-purpose programming language for data validation, ingestion, and scripting.
  
- **dbt** (data build tool): SQL-based tool for performing data transformations.
  
- **Dagster**: Data orchestration platform (similar to Apache Airflow, but asset-based instead of task-based).
  
- **SQLite**: Lightweight, local SQL database (built into Python's standard library). (In production we use Databricks; this is just a replacement for the purpose of the coding challenge.)

## Setup Guide

### Prerequisites
- **macOS or Linux**: The setup commands below assume a Unix-like environment. Windows users should adapt paths and commands as needed, or use a unix command-line environment built for Windows like Git Bash or WSL 2.
- **Python 3.10-3.13**: dbt doesn't yet support Python 3.14 (the newest release) -- if `python3 --version` on your machine reports 3.14 or newer, install an older interpreter (e.g. `brew install python@3.13` on macOS, or via [pyenv](https://github.com/pyenv/pyenv)) and point Poetry at it with `poetry env use python3.13` before running `poetry install`.
- An IDE (VS Code, PyCharm, etc.)
- AI assistant (GitHub Copilot, Claude Code, ChatGPT/Claude/Gemini via a web browser, etc.) — **you're encouraged to use it**

### Installation

Quick setup with `poetry`:

1. If you're reading this, you've already downloaded the zip file from the email attachment, and extracted it.

2. Now open a terminal window and `cd` to the location of the `mmm-challenge` starter repo within the extracted zip file.

3. **Install `poetry`** (Python dependency manager):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```
   Then restart your terminal.

4. **Install dependencies**:
   ```bash
   poetry install
   ```

5. **Verify setup**:
   ```bash
   poetry run python -c "import dbt; import dagster; import sqlite3; print('✓ All packages installed')"
   poetry run dg --version
   ```

You're ready to go!

### Running the Pipeline

Once setup is complete:

```bash
# Test dbt alone
cd dbt
poetry run dbt run
poetry run dbt test

# Run Dagster UI (full orchestration)
cd ..
poetry run dg dev
```

Then visit `http://localhost:3000` to see the Dagster UI.

### Starter Repo Structure
```
*mmm-challenge/
├── INSTRUCTIONS.md          # Challenge instructions
├── README.md                # Your documentation
├── pyproject.toml           # Dependencies + dg project config (poetry)
├── dbt/
│   ├── dbt_project.yml      # dbt config
│   ├── models/              # Your dbt models/tests
├── src/
│   └── mmm_challenge/       # Dagster project package (dg CLI convention)
│       ├── definitions.py   # Entry point -- auto-loads defs/, shouldn't need to edit
│       ├── defs/            # Your Dagster assets/jobs go here
│       └── ingest.py        # Your CSV ingest logic (Python)
└── tests/
    └── test_pipeline.py     # Your unit/integration tests
data/
├── raw/                     # Input files
└── output/                  # Write mart CSV here when done!*
```

### Running the Pipeline
```bash
# dbt alone (for testing transforms)
cd dbt
poetry run dbt run
poetry run dbt test

# Full Dagster orchestration
poetry run dg dev  # Runs UI on localhost:3000; trigger manually or set weekly schedule
```

`dg dev` auto-discovers whatever you define under `src/mmm_challenge/defs/` -- no flags needed. Add your asset modules there (e.g. `defs/assets.py`) and import your ingest logic with `from mmm_challenge.ingest import ingest_all`.

## Evaluation Criteria

You'll be assessed on:
- **Code quality**: Clear naming, type hints, modular functions
- **dbt structure**: Proper staging/marts separation, idempotency, documentation
- **Dagster DAG**: Explicit dependencies, clear asset lineage
- **Testing**: At least one meaningful test; ideally validates output correctness
- **Documentation**: README is clear; code has comments where logic isn't obvious
- **Correctness**: Mart output matches spec (correct aggregations, no data loss)

## Tips & Reminders

- **Use AI freely**—you're expected to. But understand what you build; you'll explain it in the interview.
- **Progress over perfection**—this is a 1-2 hour challenge. A solid end-to-end solution beats perfect individual components.
- **Commit as you go**—meaningful commit messages help us see your thinking.
- **Stuck?** Check if you're overcomplicating it. Mart logic is straightforward; focus on clean structure.

### Stretch Goals (if you finish early)
- Implement dbt incremental models (only process new weeks)
- Add robust data quality tests (dbt data tests and Python `pytest`)
- Set up Dagster asset partitioning by week
- Add error handling & logging to ingest script

## Deliverables

At your technical interview, bring:
1. Git repo (with commit history) hosted on GitHub or provided as ZIP
2. `mmm_mart.csv` output file
3. Be ready to walk another engineer through your work: What did you build? Why those design choices? How did you use AI?

## Questions?

If having issues setting up your IDE for the challenge, email us immediately so we can resolve quickly!

Otherwise, please ask questions in the interview. There are no "gotchas"—this is designed to be realistic, not tricky.

Good luck!
