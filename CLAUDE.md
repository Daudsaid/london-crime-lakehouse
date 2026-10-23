# CLAUDE.md — UK Crime Lakehouse

Read this fully before doing anything. These rules override your defaults.

## 1. Purpose
A portfolio data engineering project for UK Databricks Data Engineer roles: a local PySpark +
Delta Lake ELT lakehouse on UK police crime data (data.police.uk), designed to move to
Databricks later (Auto Loader, Lakeflow Declarative Pipelines, Unity Catalog, Lakeflow Jobs).
The owner must be able to explain every line in an interview, so prefer clear and idiomatic
over clever.

## 2. Environment
- macOS, Java 17, uv, Python 3.12 (pinned in .python-version). Run everything with `uv run`.
- Never use pip, never change the Python version, never add a dependency without asking.
- Commands: `uv run pytest` · `uv run ruff check .` · `uv run ruff format .` · `make <target>`

## 3. Architecture (ELT, medallion)
src/uk_crime/
- config.py: every path and setting; env-var overridable. Nothing else hard-codes paths.
- spark.py: local SparkSession with Delta (local[*], shuffle partitions 8, UTC).
- extract.py (E): download archives → data/landing/month=YYYY-MM/, files untouched.
- bronze.py (L): landing CSVs → bronze Delta, all columns string + metadata. No cleaning.
- silver.py (T): typed, cleaned, deduplicated, quality-checked; bad rows → quarantine.
- gold.py (T): Spark SQL star schema (fct_crimes, dim_*) and marts.
Data lives in data/ (gitignored): landing/, lakehouse/{bronze,silver,gold}/.

### Layer contracts (strict)
- Extract only moves bytes. It never parses, renames or filters columns.
- Bronze only adds metadata (_source_file, _ingested_at, _month). It never drops, casts or
  fixes data. Bronze is the replayable source of truth.
- All business logic lives in silver and gold. Gold reads only from silver, never bronze.

## 4. Data source facts
- Monthly archives: https://data.police.uk/data/archive/YYYY-MM.zip (large, all forces).
- Archives overlap (rolling ~36-month window), so the same crime appears in many archives,
  sometimes with an updated outcome. The newest archive wins.
- Anti-social behaviour rows have no Crime ID.
- Never guess the schema, file names or zip layout. Inspect real data first and record what
  you find here under "Verified source schema".

## 5. Reliability guardrails (non-negotiable)
Idempotency and determinism
- Re-running any step for the same month gives the same result, with no duplicates.
- Month-level writes use Delta `replaceWhere` on the month; never blind `append` for
  re-runnable loads, never full-table overwrite to update one month.
- Dedupe with an explicit, total ordering (window + row_number with a deterministic
  tie-breaker). Never rely on row order, `dropDuplicates` choosing "any" row, or
  `monotonically_increasing_id` as a key.
- Surrogate keys: `sha2` over stable business fields, with nulls coalesced to an explicit
  sentinel first (concat_ws silently skips nulls and causes collisions).

No silent data loss
- Every row from bronze ends up in silver or in quarantine with a `_dq_reason`. Log the counts
  and assert bronze_rows == silver_rows + quarantined_rows + deduped_rows for the month.
- Fail loudly: raise on unexpected states. No bare `except`, no `except Exception: pass`.

Schemas
- Explicit StructType for every read (bronze: all StringType). No schema inference.
- Delta schema enforcement stays on; no `mergeSchema` unless discussed and documented.

Extraction
- Stream downloads to a temp file, verify (size > 0, `zipfile` opens, testzip passes), then
  atomically rename into place. Never hold a whole archive in memory.
- requests: always set timeouts; retry with exponential backoff on 5xx, timeouts and
  connection errors; never retry on 4xx. Skip months already landed unless `--force`.

Spark hygiene
- Prefer built-in functions over Python UDFs.
- No `collect()`, `toPandas()` or `count()` inside loops on large data; use them only for
  small results or explicit checks.
- All timestamps in UTC; months stored as DATE (first of the month).

## 6. Code standards
- Type hints on all functions; short docstrings stating inputs, outputs and side effects.
- Transformations are pure functions: DataFrame in, DataFrame out. I/O (read/write) stays
  in thin wrapper functions so the logic is unit-testable.
- Use `logging` (module-level logger), not print. Log row counts at each layer boundary.
- Each layer is runnable as `uv run python -m uk_crime.<layer> --month YYYY-MM` (and
  `--all`); exit non-zero on failure.
- No secrets or credentials in code. No hard-coded absolute paths.
- Code must pass `ruff check` and `ruff format`. Don't add `noqa` without a written reason.

## 7. Testing
- pytest with a session-scoped local Spark fixture in tests/conftest.py.
- Tests use small in-memory or tests/fixtures data. Tests never touch the network or data/.
- Each transformation gets tests for: the happy path, nulls/blank strings, a duplicate
  crime across two archives (newest wins), a row with no Crime ID, and a row that should be
  quarantined.
- Never weaken, delete or skip a test to make it pass. If a test seems wrong, stop and ask.

## 8. Git workflow
- One branch per stage (feat/…, fix/…, docs/…). Conventional commit messages.
- Never commit data/, .venv/ or generated files.
- Never push, merge, rebase, force-push or delete branches or files without asking first.

## 9. How to work with me
- Work one stage at a time. For each stage: write the code, run it, run pytest and ruff,
  show me the real output, then STOP and wait for my OK before committing or moving on.
- Never claim something works unless you ran it in this session and saw it pass.
- Explain non-obvious decisions briefly so I can defend them in interviews.
- If something is unclear or the data contradicts this file, ask; don't improvise.
- Keep scope tight: no extra features, abstractions or files beyond the current stage.

## 10. Databricks portability
- Keep paths derived from config.DATA_DIR (it becomes a Unity Catalog Volume path).
- Keep transformations as plain DataFrame functions so they drop into Lakeflow Declarative
  Pipelines; local landing → bronze maps to Auto Loader; quarantine maps to expectations.
- Don't depend on anything local-only (e.g. file system listing tricks) inside transforms.

## 11. Definition of done (every stage)
- [ ] Runs end-to-end for at least one real month
- [ ] Re-running gives identical row counts (idempotent)
- [ ] pytest passes; ruff check and ruff format clean
- [ ] Row counts logged and reconciled
- [ ] Decisions explained; this file updated if anything changed

## Verified source schema
(To be filled in after inspecting a real archive.)
