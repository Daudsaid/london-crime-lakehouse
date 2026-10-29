# CLAUDE.md — London Crime Lakehouse

Read this fully before doing anything. These rules override your defaults.

## 1. Purpose
A portfolio data engineering project for UK Databricks Data Engineer roles: a local PySpark +
Delta Lake ELT lakehouse on Metropolitan and City of London Police crime data
(data.police.uk), designed to move to Databricks later (Auto Loader, Lakeflow Declarative Pipelines, Unity Catalog, Lakeflow Jobs).
The owner must be able to explain every line in an interview, so prefer clear and idiomatic
over clever.

## 2. Environment
- macOS, Java 17, uv, Python 3.12 (pinned in .python-version). Run everything with `uv run`.
- Never use pip, never change the Python version, never add a dependency without asking.
- Commands: `uv run pytest` · `uv run ruff check .` · `uv run ruff format .` · `make <target>`
- `make help` lists the targets (extract, bronze, silver, gold, pipeline, test, test-all,
  lint, format, profile). `make test` is the fast set (`pytest -m "not slow"`); end-to-end
  tests that write real Delta tables are marked `@pytest.mark.slow`; `make test-all` and
  plain `uv run pytest` run everything. The test session uses a small Spark config
  (tests/conftest.py via `get_spark(overrides)`); production defaults are unchanged.
- CI (.github/workflows/ci.yml): every push runs ruff and the fast tests; pull requests to
  main also run the slow tests. Tests must never need data/ or the network.

## 3. Architecture (ELT, medallion)
src/uk_crime/
- config.py: every path and setting; env-var overridable. Nothing else hard-codes paths.
- spark.py: local SparkSession with Delta (local[*], shuffle partitions 8, UTC).
- extract.py (E): download archives → data/landing/archive=YYYY-MM/month=YYYY-MM/, files untouched.
- bronze.py (L): landing CSVs → bronze Delta, all columns string + metadata. No cleaning.
- silver.py (T): typed, cleaned, deduplicated, quality-checked; bad rows → quarantine. A full
  rebuild from bronze on every run (see section 5). Tables: SILVER_DIR/crimes_street and
  SILVER_DIR/crimes_street_quarantine.
- gold.py (T): Spark SQL star schema and marts, a full rebuild from silver on every run (one
  atomic overwrite per table under GOLD_DIR). Dimensions dim_date (month grain, UK financial
  year Apr-Mar), dim_force, dim_crime_type (explicit severity_tier mapping), dim_outcome
  (status from the silver lists, explicit outcome_group mapping), dim_lsoa (local_authority =
  lsoa_name without its trailing code); each has an UNKNOWN member so fact keys are never
  NULL. fct_crimes is one row per silver record. Marts: mart_monthly_crimes,
  mart_outcome_rates (non-ASB; leaves out the newest OUTCOME_MATURITY_MONTHS = 16 months
  and OUTCOME_EXCLUDED_MONTHS = 2024-10, and records the months used and excluded) and
  mart_lsoa_ranking (newest LSOA_RANKING_MONTHS = 12 months, London LSOAs only via
  dim_lsoa.is_london, crime_count excludes ASB with asb_count alongside, ranked within
  local_authority, counts not rates). Borough marts (London LSOAs only, so rows outside
  London and rows with no LSOA are the difference to fct_crimes): mart_borough_monthly
  (local_authority, month, crime_type, is_asb, crime_count) and mart_borough_outcomes (same
  window, exclusions and non-ASB rule as mart_outcome_rates; share within borough and crime
  type). is_london is a name match against LONDON_AUTHORITIES
  (known limitation, see docs/data-caveats.md). All tables carry _silver_run_id; gold
  calls assert_silver_consistent() first and assert_gold_consistent() checks that all eleven
  tables share one _silver_run_id. An unmapped force, crime type or outcome raises.
  Outcome maturity rule: exclude the smallest number of most-recent months so that every
  remaining month newer than the baseline window has a placeholder share within 2pp of the
  baseline (median of months older than 18 months). Set from archive 2026-08; re-check
  when a new archive lands (analyses/profile_outcome_maturity.py, evidence in
  docs/data-caveats.md). 2024-10 (25.6% placeholder share) is excluded from this mart only.
- export.py: after gold.assert_gold_consistent(), writes a small Parquet snapshot of five marts
  (plus LSOA centroids), a reconciliation table from silver's run summary
  (SILVER_DIR/run_summary.json, written at the end of each silver run) and snapshot.json to
  dashboard/data/ (committed, under 1 MB). It refuses a run summary from a different silver
  run than the one gold was built from.
- dashboard/app.py: Streamlit page that reads only dashboard/data/ (no Spark, no data/). A
  sidebar borough selector (All London + the 33 boroughs) is synced to ?borough=<name>; an
  unknown name falls back to All London.
  Its dependencies are in the separate `dashboard` uv group (streamlit, plotly); the
  pipeline's own dependencies are unchanged. app.py must not import
  uk_crime or pyspark; dashboard/requirements.txt (generated with `uv export`, see its header)
  lets it be deployed on its own. `make export` refreshes the snapshot, `make dashboard` runs it.
pandas pin: the `dashboard` group pins `pandas<3` in pyproject.toml because PySpark 4.2 does not
yet fully support pandas 3 (it warns on import of its testing utilities). pytest turns that
FutureWarning (that message only) into an error, so a pandas 3 install fails the tests and CI.
Dependabot's uv entry uses `versioning-strategy: lockfile-only`, so it only updates uv.lock and
never edits constraints such as this pin; lifting the pin is a manual decision.
Data lives in data/ (gitignored): landing/, lakehouse/{bronze,silver,gold}/.

### Layer contracts (strict)
- Extract only moves bytes. It never parses, renames or filters columns.
- Bronze only adds metadata. It never drops, casts, cleans or dedupes data. Bronze is the
  replayable source of truth. One Delta table, BRONZE_DIR/crimes_street, partitioned by
  _archive_month, loaded per archive with replaceWhere. Columns: the 12 source columns as
  strings (renamed to snake_case only because Delta rejects spaces: crime_id, month,
  reported_by, falls_within, longitude, latitude, location, lsoa_code, lsoa_name,
  crime_type, last_outcome_category, context), _corrupt_record, plus metadata parsed from
  the file path with anchored regexes (no partition discovery): _archive_month DATE,
  _month DATE, _force STRING, _source_file STRING (path relative to LANDING_DIR, e.g.
  archive=2026-08/month=2026-08/2026-08-metropolitan-street.csv), _ingested_at TIMESTAMP
  (one value per run). Blank CSV cells are NULL. The CSV header is checked against the
  schema (enforceSchema=false), so header drift fails the load.
- All business logic lives in silver and gold. Gold reads only from silver, never bronze.

## 4. Data source facts
- Monthly archives: https://data.police.uk/data/archive/YYYY-MM.zip (large, all forces except Greater Manchester).
- Archives overlap (rolling ~36-month window), so the same crime appears in many archives,
  sometimes with an updated outcome. The newest archive wins.
- Anti-social behaviour rows have no Crime ID.
- Never guess the schema, file names or zip layout. Inspect real data first and record what
  you find here under "Verified source schema".

## 5. Reliability guardrails (non-negotiable)
Idempotency and determinism
- Re-running any step for the same month gives the same result, with no duplicates.
- Bronze loads use Delta `replaceWhere` on the archive month; never blind `append` for
  re-runnable loads, never full-table overwrite to update one archive.
- Silver is the exception: every archive restates all 36 months, so silver is a full
  rebuild from bronze on every run, written as one atomic Delta overwrite per table
  (quarantine first, then silver). It is deterministic, so re-runs give identical output
  (checked by row count and an order-independent checksum over record_key and over all
  silver columns except _run_id).
- Two-table consistency pattern: the two overwrites are separate Delta commits, so a crash
  between them leaves new quarantine beside old silver. Every row in both tables carries
  `_run_id` (one UUID per run; also written as the Delta commit's userMetadata, which
  identifies the run of an empty table). `silver.assert_silver_consistent()` raises unless
  both tables hold exactly one `_run_id` and it is the same one; gold must call it first.
- Dedupe rule (source-specific). Newest archive wins at file grain: for each (force, data
  month), silver uses the file from the newest archive containing it; rows from older
  archives for that (force, month) are superseded wholesale (counted as deduped_rows).
  Within-file duplicate Crime IDs (grain: _force, _month, crime_id within the selected
  archive):
  1. Rows identical across all 12 source columns collapse to one -> deduped_rows.
  2. If the distinct variants differ and exactly ONE distinct variant has a progressed
     outcome (all others are placeholders), keep it; the others are superseded_rows. Set
     dq_attribute_conflict = true on the kept row if crime_type, location, lat/long or LSOA
     differed among the variants.
  3. Otherwise quarantine all copies with _dq_reason = 'conflicting_duplicate_crime_id'.
  4. ASB rows (no Crime ID) are never deduplicated: surrogate key = sha2(force, month, row
     fields) + occurrence number.
  Outcome vocabulary: two explicit named lists in silver.py, never inferred.
  PLACEHOLDER_OUTCOMES = NULL, "Status update unavailable", "Under investigation".
  PROGRESSED_OUTCOMES = every other value observed in 2026-08: "Investigation complete; no
  suspect identified", "Unable to prosecute suspect", "Court result unavailable", "Local
  resolution", "Awaiting court outcome", "Action to be taken by another organisation",
  "Offender given a caution", "Offender given penalty notice", "Formal action is not in the
  public interest", "Further investigation is not in the public interest", "Further action
  is not in the public interest", "Offender given a drugs possession warning", "Suspect
  charged as part of another case". "Awaiting court outcome" is progressed because it
  records a real action (the suspect was charged; in the outcomes files it is "Suspect
  charged"), not a missing status. A value in neither list must raise with the value, so a
  new police.uk category is a visible decision, not a silent misclassification.
  Rationale and evidence: docs/duplicate-crime-ids.md. The outcomes-file cross-check there
  is validation only, never a silver input.
- Dedupe with an explicit, total ordering (window + row_number with a deterministic
  tie-breaker). Never rely on row order, `dropDuplicates` choosing "any" row, or
  `monotonically_increasing_id` as a key.
- Surrogate keys: `sha2` over stable business fields, with nulls coalesced to an explicit
  sentinel first (concat_ws silently skips nulls and causes collisions).

No silent data loss
- Every row from bronze ends up in exactly one class. Log each class per run and assert
  bronze = silver + quarantined + deduped + superseded + archive_replaced, where
  deduped = identical copies within a file; superseded = placeholder copies replaced by
  the progressed variant (R5); archive_replaced = rows from older archives' copies of a
  (force, month) file. Quarantined rows keep the raw bronze row and a single `_dq_reason`.
- Silver row-level DQ uses the FIRST failing rule in this fixed order: corrupt_record,
  invalid_month (unparseable, or different from the _month in the file path),
  unexpected_crime_id_presence (ASB with an ID, or non-ASB without one),
  invalid_coordinates (unparseable, one of the pair missing, or outside the UK box in
  silver.py). NULL coordinates are valid: the row is kept with has_location = false.
  Duplicate handling (R5, above) then runs on DQ-passing rows.
- Vocabulary drift raises, it never quarantines: an outcome in neither named list or a
  crime_type outside CRIME_TYPES stops the run with the value.
- Quarantine guardrail: if any single quarantine reason exceeds 0.5% of the rows considered,
  silver writes nothing and raises with the counts.
- Fail loudly: raise on unexpected states. No bare `except`, no `except Exception: pass`.

Schemas
- Explicit StructType for every read (bronze: all StringType). No schema inference.
- Delta schema enforcement stays on; no `mergeSchema` unless discussed and documented.
- Derived tables (silver, quarantine, later gold) may change schema only through an explicit
  opt-in: their expected StructType is defined in code, and if the existing table differs
  the run fails naming the added, removed and changed columns and telling you to re-run
  with `--allow-schema-change`. That flag logs the diff at WARNING and rebuilds the table
  with `overwriteSchema` (safe because they are fully derived from bronze). Bronze never
  changes schema this way, and the flag is never the default.

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
- Each layer is runnable as `uv run python -m uk_crime.<layer>`; exit non-zero on failure.
  extract: `--archive YYYY-MM` or `--latest` (plus `--force`); bronze: `--archive YYYY-MM`
  or `--all`; silver and gold: `--all` (always a full rebuild; `--allow-schema-change` is the
  only way to change an existing derived table's schema).
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
Inspected 2026-10-23 on archive 2026-08.zip (1,733,535,664 bytes compressed, ~7.6 GB
uncompressed, testzip OK). Re-verify if the source changes.

Archive
- `archive/latest.zip` 302-redirects to `https://policeuk-data.s3.amazonaws.com/archive/YYYY-MM.zip`
  (HEAD works, `Accept-Ranges: bytes`). Latest on 2026-10-23 was 2026-08; 2026-09 and 2026-10
  return 404. Archives are ~1.7 GB each.
- Layout: 4,489 entries, no directory entries, one folder per data month:
  `YYYY-MM/YYYY-MM-<force>-<kind>.csv`, kind = `street` | `outcomes` | `stop-and-search`.
- Window: exactly 36 data months (2023-09 .. 2026-08): the archive month and the 35 before it.
- 44 force slugs, lower-case-hyphenated (includes `btp`, `northern-ireland`, `city-of-london`,
  `metropolitan`). Greater Manchester is absent entirely. `btp` has no street file from
  2025-02 on; `gloucestershire` has none from 2026-02 on. The force set varies by month, so
  never assume 44 street files per month. Not every force has every kind every month.
- Met: `2026-08/2026-08-metropolitan-street.csv` and `...-outcomes.csv` (no stop-and-search
  that month). City of London: `...-city-of-london-street|outcomes|stop-and-search.csv`.
- Files are UTF-8, no BOM, comma-delimited, quoted, header row, rectangular.

Street CSV (12 columns, in order)
Crime ID, Month, Reported by, Falls within, Longitude, Latitude, Location, LSOA code,
LSOA name, Crime type, Last outcome category, Context
- `Month` is `YYYY-MM` and equals the folder/file month in the files checked.
- Met 2026-08: 94,515 rows; 21,499 blank Crime ID (all Anti-social behaviour); 73,015 distinct
  IDs; 1 Crime ID appears twice inside the same file (Met 2023-09 also has repeats), so
  dedupe must also handle duplicates within a single file.
- Blanks are empty strings. `Last outcome category` is blank for ~22% of Met rows.

Outcomes CSV (10 columns)
Crime ID, Month, Reported by, Falls within, Longitude, Latitude, Location, LSOA code,
LSOA name, Outcome type
- Met 2026-08: 76,778 rows, 0 blank Crime ID, 73,701 distinct (a crime can have several
  outcome rows). Separate event stream; out of scope until after street.

Stop-and-search: not inspected (out of scope).

Data-quality facts that drive the rules above
- Greater Manchester is absent from the archive; the force set varies by month (`btp` and
  `gloucestershire` gaps). Extract must handle per-month gaps, not assume a fixed set.
- ASB rows have no Crime ID. Identical ASB rows are distinct incidents (locations are
  anonymised to snap points), so they must never be deduplicated. Met 2026-08: 21,499 ASB
  rows but only 11,990 distinct, with 13,854 rows in repeated groups.
- Met 2026-08 has one Crime ID appearing twice within the same file (lines 47212 and 47214),
  and the two rows are identical in every column:
  `9eccc281...b6854 | 2026-08 | Metropolitan Police Service | Metropolitan Police Service |
  -0.372945 | 51.457471 | On or near Wellington Road South | E01002652 | Hounslow 021D |
  Public order | Under investigation | (blank)`.

Bronze survey: Crime IDs repeating within a file (archive 2026-08, run 2026-10-23)
- Bronze holds 3,440,762 rows from 72 files (metropolitan, city-of-london; 36 months each),
  0 rows in _corrupt_record.
- 47 of the 72 files contain repeated Crime IDs: 21,564 repeated IDs, 24,090 extra rows.
  19,861 of those 21,564 groups (92%) are NOT identical across the 12 source columns.
- Where they differ (a group can differ in several columns): last_outcome_category 19,294
  (14,978 differ in that column only), crime_type 4,626, longitude/latitude ~257,
  location 255, lsoa_code/lsoa_name 137. Never month, reported_by, falls_within or context.
- Pattern: Metropolitan repeats are heavy in 2023-09..2024-02 (about 1,200-3,400 IDs per
  file) and 2025-06..2026-04 (about 500-850 per file), and absent in 2024-03..2025-04;
  City of London has them in 2023-09..2025-05 (6-27 per file) and a few later. The one
  repeat in Met 2026-08 is the identical pair above. Most repeated groups have 2 rows
  (max 10).
- Consequence: quarantine-all would have quarantined 19,861 Crime IDs (42,019 rows, 1.5% of
  the 2,738,089 rows that have a Crime ID) in this archive, mostly because
  last_outcome_category differs. The outcome-precedence rule above quarantines 599 Crime IDs
  (1,279 rows) instead; see docs/duplicate-crime-ids.md.
