# ADR 001: Local Pipeline Design

**Date:** 2026-11-01 (updated 2026-11-02)
**Status:** Implemented: ingestion, raw loading, cleaning, modeled checks, the
annual output table, and a read-only results notebook.

## Context

Build a small, reproducible WPRDC pipeline for a take-home exercise. Keep setup
simple and make each decision easy to explain.

## Decisions

| Area | Decision and reason |
| --- | --- |
| Dataset and question | Compare median market-sale prices by municipality/year starting in 2020; rank annual percentage changes when both adjacent years have at least 30 qualifying sales. The user chose year-by-year rather than pooled-period comparisons. |
| Runtime and dependencies | Use Python 3.12, `.venv`, and pip with pinned direct dependencies in [requirements.txt](../requirements.txt). This keeps setup small; it does not lock indirect dependencies. |
| Ingestion and modeling | Use Python and requests for downloads; DuckDB SQL for cleaning and analysis. Prefer this over pandas-only transformations for readable analytical logic. |
| Local database | Use DuckDB without a server. Postgres and Docker add setup we do not need here. |
| Raw data | Download the combined CKAN resource as one full CSV and keep its bytes unchanged. Overwrite the latest file only after validation, with separate JSON source metadata. Reject snapshot retention and hashes because history is not needed for this exercise; previous results cannot be reconstructed. No pagination fallback is implemented. |
| Raw loading | Load the local CSV into `raw.property_sales` with all 26 columns as `VARCHAR`, preserving blanks, leading zeros, and source duplicates. Reject type inference or cleaning during loading so source evidence remains available for EDA. Require valid ingestion metadata and validate the CSV before opening DuckDB. |
| CSV parsing | The dump mixes a CRLF header with LF records, which DuckDB 1.4.2 rejects in strict mode. Use DuckDB `strict_mode=false` only with strict Python preflight, no error skipping or padding, and table schema/count checks. A source-specific regression and full-data field comparison verify preservation. |
| Reruns and updates | Ingestion uses checked staged replacement; loading replaces raw transactionally. Modeling rebuilds cleaned and output together in a separate checked transaction, reflecting additions, corrections, and deletions without append/upsert logic. A modeled failure does not undo a raw refresh. |
| Data quality | Use Python/SQL checks and pytest for parsing, keys, code/mapping drift, eligibility, output integrity, and failure/recovery. Exact medians, changes, and ranks are covered by regression tests rather than recomputed at runtime. Dataset-driven gates cover recording-date anomalies, placeholder groups, and ward roll-up. Reject dbt/Great Expectations overhead for this small pipeline. |
| Sale rules | Code 0, price at least $1,000, and no known multi-parcel deed. Count real deeds before filtering. Include but flag unknown references without claiming proven single-parcel status. Keep all rows in cleaned; no automatic removal of high prices. These are not property valuations or exclusively residential sales. |
| Identity and precision | Preserve original deed values for natural keys: placeholder nulling collapses 15 source-key groups. Use normalized references only for grouping. Three-decimal median precision preserves half-cent averages. |
| Repository layout | Keep Python pipeline stages in `src/`, SQL transformations in `src/sql/`, exploration in root-level `notebooks/`, and tests in `tests/`. Use `data/raw/` and `data/duckdb/`, with `.gitkeep` files so they exist after cloning. |
| Commands | Use Make with the virtual environment: `make ingest`, `make load`, and `make transform` are separate stages, and `make run` runs them in order, stopping on failure. Modeling is local-only and does not automatically reload raw. Retain install/test/help/cache commands; cleanup must not delete data. Open the notebooks separately. |
| Exploration | Use Jupyter and pandas to inspect the source. Move final cleaning rules and checks into the pipeline so production runs do not depend on the notebook. |
| Results presentation | Use [02_market_price_analysis.ipynb](../notebooks/02_market_price_analysis.ipynb) to read validated DuckDB output and visualize annual medians, sample coverage, and growth ranks. A notebook keeps code, charts, and interpretation together without an app server, hosting, or new dependencies. |
| Published data | The required output is `output.municipality_year_sales` in DuckDB. Keep raw, metadata, and DuckDB ignored. Notebook outputs provide reviewable charts and tables; no CSV export or extra analytical table is needed. |
| Documentation | Keep project goals, decisions, tradeoffs, and AI disclosure in the README; put pipeline mechanics in [pipeline.md](pipeline.md). Maintain this single ADR. |
| Scope and disclosure | No Airflow, distributed processing, valuation model, or causal claims for this exercise. Disclose GitHub Copilot use in the [README](../README.md); do not report unverified findings. |

## When to Revisit

- Use uv or Poetry in a real project for dependency locking and environment management.
- Add dbt and dbt tests when models and their dependencies become harder to manage.
- For production daily refreshes, consider Airflow, alerts, source-change checks,
  and retention policies. A simple scheduler may still suit one independent job.
- Add locking for concurrent writers and stronger freshness verification when
  timestamp/count checks are insufficient. Retain raw history and hashes when
  audits or reproduction of earlier results are required, regardless of whether
  the source continues doing full reloads.
- Measure runtime, memory, and shared access needs before increasing compute or
  changing databases. A 100x increase alone does not require a new platform.
- Use S3 or similar storage and Parquet when useful; storage does not replace a
  query engine. Add incremental loads only with verified keys and update handling,
  including corrections, deletions, and periodic full comparisons.
