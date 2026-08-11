# AGENTS.md

Guidance for humans and AI agents changing this repo. Read before editing.

## Overview

An end-to-end financial data pipeline: Tiingo API → **Landing → Bronze → Silver → Gold**
in PostgreSQL (Supabase). `run_pipeline.py` runs each step as a separate subprocess in
order and aborts on the first failure. Gold is 11 read-only SQL views; the earlier layers
are tables.

## Build & test

```bash
pip install -r requirements.txt        # deps
python -m pytest -q                    # 21 tests, mocked, no DB needed
python run_pipeline.py                 # full run — requires .env (DB_* + TIINGO_API_KEY)
# individual steps:
python fetch_stocks.py                 # → landing
python load_bronze.py                  # landing → bronze
python load_silver.py                  # bronze → silver
python load_dimensions.py              # dim_ticker
python load_gold.py                    # (re)create the 11 gold views
```

## Code style (only where it differs from Python defaults)

- **Logging, never `print`.** Use `from config import logger`; all output goes through it.
- **`date` must be a pandas datetime, never a string.** Landing is created from the
  DataFrame's dtypes — a string `date` becomes a TEXT column and breaks the bronze
  `date > MAX(date)` comparison.
- **Columns are lowercase snake_case**, matching `db.BRONZE_COLUMNS` / `db.SILVER_COLUMNS`.
- **Each `load_*.py` is a top-level script** (`try/except … raise`), not a function — only
  `fetch_stocks.py` has a `main()`. A new step keeps that shape and is added to
  `run_pipeline.py`'s `STEPS`.
- **All data-movement SQL lives in `db.py`.** Scripts call its helpers; don't inline
  `to_sql`/`read_sql` in load steps. (View DDL in `load_gold.py` and table DDL in
  `load_silver.py`/`load_dimensions.py` are the deliberate exceptions.)

## Architectural constraints (per layer — do not break)

- **Ingestion fails loudly.** A run that loads zero rows on a trading day is a
  **failure**, not "already up to date": `fetch_stocks.py` retries the source and
  then **raises** when a weekday window returns no data. Never let an empty fetch
  exit 0 or pass as success.
- **Landing** is **dropped and recreated every run** (`db.replace_table`,
  `to_sql(if_exists="replace")`). Any table-level setting — column types, RLS, indexes —
  must be re-applied *in code* after the rebuild, never set once in the DB. RLS is
  re-enabled this way; do not remove that.
- **Bronze** is **append-only and idempotent** via `INSERT … ON CONFLICT (ticker, date)
  DO NOTHING` (`db.insert_ignore_duplicates`), backed by a unique index. Duplicates are
  *skipped*. Never truncate, `replace`, or rewrite bronze.
- **Silver** appends via plain `to_sql` **plus** a `uq_silver_ticker_date` unique index —
  so a duplicate here **errors** (it does *not* silently skip like bronze; the two layers
  are not the same pattern). Silver **flags, never deletes**: `validate_row()` sets a
  `status` column (`valid` / `invalid_*`); invalid rows are kept, not dropped or fixed.
- **Why bronze skips but silver crashes:** a bronze duplicate is routine and harmless
  (overlapping Tiingo fetch windows), so it's skipped silently; a silver duplicate means
  the incremental cutoff (`get_max_date`) is wrong — a bug — so it must fail loudly and
  stop the pipeline rather than pass unnoticed.
- **Gold** is **11 SQL views**, not tables (`CREATE OR REPLACE VIEW … WITH
  (security_invoker = true)`), each filtering `WHERE s.status = 'valid'`. Never materialize
  a gold view into a table, never write to one, and keep `security_invoker = true`.
- **Dimensions**: `dim_ticker` is **upserted** (`ON CONFLICT (ticker) DO UPDATE`). Keep the
  upsert; don't switch to insert-or-truncate.

## Definition of done

- `pytest` is green (`python -m pytest -q`).
- New behavior has a test — especially anything in the fetch or validation path.
- Tests run **without a database**: mock the data source and DB, never require a
  live connection.
- The architectural constraints above still hold.

## Data contract

Gold view columns are defined inline in **`load_gold.py`** (the 11 `CREATE OR REPLACE VIEW`
statements). Treat that file as the source of truth for gold columns. *(A dedicated
`SCHEMA.md` is tracked in issue #6.)*

## Out of scope — never modify without asking first

- `.github/workflows/` (CI + secrets wiring)
- Credential handling in `config.py` (the `os.getenv` reads and `get_engine`)
- `.env`, `.env.example`, or anything touching database/API credentials

## For automated agents

- **Never push to `main` or self-merge.** Work on a branch and open a PR for human
  review (branch protection enforces this; this states the intent).
- **Keep fixes minimal and reversible** — diagnose the root cause and change the
  least code that resolves it.
- **If the root cause is environmental, not a code bug** (e.g. the data source is
  unreachable or rate-limiting CI), say so in the PR rather than inventing a code
  change to paper over it.

## Security

Credentials come only from environment variables (`config.py`), never hardcoded. Never log
the DB password or `TIINGO_API_KEY`, and never log the full connection URL (it embeds the
password). Never remove or bypass the validation `status` system — invalid rows must stay
flagged in silver, not deleted, so gold's `WHERE status = 'valid'` filter remains the single
quality gate.
