# London Crime Lakehouse. Run `make help` for the list of targets.
# Pass extra CLI arguments with ARGS, e.g. make silver ARGS=--allow-schema-change

.DEFAULT_GOAL := help
ARGS ?=

.PHONY: help extract bronze silver gold export dashboard explore requirements pipeline test test-all lint format profile

help: ## List the available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

extract: ## Download the latest archive and land its street CSVs
	uv run python -m uk_crime.extract --latest $(ARGS)

bronze: ## Load every landed archive into the bronze Delta table
	uv run python -m uk_crime.bronze --all $(ARGS)

silver: ## Rebuild silver (and quarantine) from bronze
	uv run python -m uk_crime.silver --all $(ARGS)

gold: ## Rebuild the gold star schema and marts from silver
	uv run python -m uk_crime.gold --all $(ARGS)

export: ## Write the dashboard snapshot (dashboard/data/) from gold
	uv run python -m uk_crime.export $(ARGS)

dashboard: ## Run the Streamlit dashboard locally (reads dashboard/data/ only)
	uv run --group dashboard streamlit run dashboard/app.py $(ARGS)

explore: ## Start JupyterLab in notebooks/ (read-only exploration with Polars)
	uv run --group explore jupyter lab --notebook-dir notebooks

requirements: ## Regenerate dashboard/requirements.txt from uv.lock
	@{ sed -n '1,3p' dashboard/requirements.txt; \
	  uv export --locked --only-group dashboard --no-emit-project --no-hashes --no-header; \
	} > dashboard/requirements.txt.new
	@mv dashboard/requirements.txt.new dashboard/requirements.txt

pipeline: ## Run extract --latest, then bronze, silver and gold in order
	$(MAKE) extract
	$(MAKE) bronze
	$(MAKE) silver
	$(MAKE) gold

test: ## Fast tests (everything except the slow end-to-end tests)
	uv run --group dashboard pytest -m "not slow"

test-all: ## The full test suite, including the slow tests
	uv run --group dashboard pytest

lint: ## ruff check and ruff format --check (no changes)
	uv run ruff check .
	uv run ruff format --check .

format: ## Apply ruff fixes and formatting
	uv run ruff check --fix .
	uv run ruff format .

profile: ## Run both read-only analyses (need bronze/silver and the kept archive zip)
	uv run python analyses/profile_duplicate_crime_ids.py
	uv run python analyses/profile_outcome_maturity.py
