# Allegheny County Property Sale Transactions

A small, reproducible pipeline using property sale data from the
[Western Pennsylvania Regional Data Center (WPRDC)](https://www.wprdc.org/).

## Latest Run

The 2026-11-02 run (source last modified 2026-10-30) passed all quality checks
and produced 766 municipality-year rows for 2020-2025 from 57,029 qualifying
sales across 506,991 raw records. Counts are snapshot-specific; see
[docs/pipeline.md](docs/pipeline.md) for commands, checks, and recovery.

## Question

**How has the median market sale price changed by municipality and year, and
which municipalities rose fastest?**

Compare nominal recorded prices of assessor-coded valid sales with no known
multi-parcel deed, for completed years starting in 2020. Rank percentage changes
from the immediately preceding year separately for each year. Both years need
at least 30 qualifying sales to rank. 2020 is the baseline; exclude the partial
current year.

This describes selected sales, not property values or appreciation of the same
homes. Commercial properties are included: the source has no property-use field
for a residential-only filter. Unknown deed references are flagged because
single-parcel status cannot be verified for those records.

## Tech Stack

- **Python and requests:** Fetch source data and control the pipeline.
- **DuckDB and SQL:** Clean the data and build the analytical table locally.
- **pytest:** Test transformation rules and confirm that bad inputs fail checks.
- **Jupyter Notebook, pandas, and matplotlib:** Explore source data and visualize
  the validated municipality-year output.
- **pip:** Install pinned dependencies from [requirements.txt](requirements.txt)
  in a virtual environment.
- **Makefile:** Run setup, tests, the pipeline, and cache cleanup.

The local pipeline needs no cloud account, database server, or Docker setup.

I use pip for this exercise to keep setup simple. In a real project, I would use
uv or Poetry to manage environments and lock direct and indirect dependencies.
The versions in [requirements.txt](requirements.txt) pin direct dependencies only;
they are not a complete dependency lock.

## Three Main Design Decisions

### 1. Use Python for Downloads and SQL for Transformations

Python handles HTTP requests, timeouts, retries, and pipeline execution.
SQL handles type conversion, cleaning, grouping, and data quality checks.
Ingestion uses the complete resource dump, not pagination.

This keeps each part easy to read and explain. The tradeoff is maintaining a
small amount of code in two languages.

**Alternative rejected:** A pandas-only pipeline would work, but SQL makes the
analytical logic easier to review. SQL alone would make downloading and saving
raw responses harder.

### 2. Use DuckDB Without dbt

DuckDB provides an analytical SQL database in a local file. A few SQL files are
enough for this project's cleaned layer and output table.

It is simple to install and does not need a running server. It is not intended
for a shared database with many writers.

**Alternatives rejected:** Postgres with Docker adds setup without a clear
benefit here. I considered dbt and its built-in data tests for model dependencies,
validation, and documentation, but decided against them to keep this small
pipeline simple. I would reconsider dbt if the number of models or contributors
grew.

### 3. Overwrite Raw Data and Rebuild the Modeled Tables

Keep one complete raw CSV, with downloaded bytes unchanged and source metadata
stored separately. Replace it only after ingestion checks pass. WPRDC reloads
the source in full, so our refresh also replaces rather than appends.

The modeled layers are rebuilt and published together in a DuckDB transaction
after quality checks pass. This handles new records, corrections, and deletions
without incremental change tracking. A failure preserves both prior modeled
tables; it does not undo a separately committed raw refresh.

**Alternatives rejected:** Snapshot history is unnecessary for this exercise;
the tradeoff is that earlier results cannot be reconstructed once the raw file
is replaced. Appending risks duplicates and missed corrections. Upserts need
verified stable keys and deletion handling. Safe overwrite is described in
[docs/pipeline.md](docs/pipeline.md).

## Output Table

`output.municipality_year_sales` in the local DuckDB database has one row per
municipality and completed sale year with qualifying sales. Missing years have
no row, not a zero median. The current snapshot covers 2020-2025.

| Column | Meaning |
| --- | --- |
| `municipality_key`, `municipality` | Stable code-based key and normalized name, with city wards rolled up |
| `sale_year` | Year from `SALEDATE`, not recording date |
| `sale_count`, `median_sale_price` | Qualifying sale count and exact median recorded price |
| `low_sample_flag` | Fewer than 30 qualifying sales in this year |
| `previous_year_sale_count`, `previous_year_median_sale_price` | Values from the immediately preceding year, or `NULL` |
| `yoy_price_change`, `yoy_percent_change` | Dollar and percentage change from that year, or `NULL` |
| `ranking_eligible`, `yoy_growth_rank` | Both years have at least 30 sales; descending percentage-change rank within the year |

A median rising from $100,000 to $150,000 is a $50,000 / 50% increase. Ties share
a rank. Small groups remain visible but are not ranked. 2020 and years without
an adjacent previous-year observation have no change or rank. Negative changes
remain negative: rank 1 can mean the smallest decline if every municipality
declined. Ranks use unrounded values.

**Sale rules:** code `0`, price at least $1,000, and either a real deed with one
parcel row or an unknown reference. Real deed groups are calculated across the
entire source before filtering. Unknown references are never grouped together
and are flagged in cleaned; six market-coded rows have unknown references
across all years in this snapshot.

Starting in 2020 avoids crossing the observed earlier sale-code distribution
break, not every potential sample change. Recording lag means the latest
completed year's results can change on subsequent refreshes.

## Data Quality Checks

I considered dbt data tests and Great Expectations, but chose SQL checks in
DuckDB, run by Python, to keep setup and dependencies small. Great Expectations
offers reusable checks and structured validation reports, but one dataset does
not need a separate validation framework here. The tradeoff is writing clear
failure messages and reporting in Python myself.

Python fails the run when a required SQL check fails. pytest tests valid and
deliberately bad inputs, repeat runs, updates, and transactional rollback.

Ingestion already rejects schema changes, empty sources, malformed records,
incomplete downloads, and detectable source changes during a download. Detailed
checks live in
[docs/pipeline.md](docs/pipeline.md).

**Dataset-driven checks:** The loader-line/datastore-count discrepancy motivated
counting parsed CSV records. EDA found `0212-08-01`: cleaning nulls and flags
pre-2000 recording dates, and more than one fails the run. Placeholder references
must never create multi-parcel groups, and each real deed can contribute at most
one market-sale row. Ward roll-up must produce 130 municipalities.

Other checks enforce required fields, strict ISO date/decimal price parsing,
nonnegative prices, original natural-key uniqueness, known code membership,
raw-to-cleaned row conservation, output keys, year bounds, positive counts,
and conservation of qualifying sales. Errors name the check and violation count.
Exact medians, adjacent-year changes, sample eligibility, and growth ranks are
covered by focused tests rather than recalculated in runtime quality checks.

## What Surprised Me in the Data

- Sale-code usage changes sharply around 2020, particularly code `36`. This
  suggests a coding change.
- Rows represent parcels on deeds. Repeated deed prices overstate multi-parcel
  dollar totals by 3.9x if summed by row.
- $1 is the most common price. Only 19.8% of rows are coded valid, and 37 valid
  rows are below the $1,000 analysis floor.

Detailed observations and supporting queries are in
[notebooks/01_exploratory_data_analysis.ipynb](notebooks/01_exploratory_data_analysis.ipynb).

## Reruns and Source Updates

`make run` executes `make ingest`, `make load`, and `make transform` in order.
Ingestion skips an unchanged source (same resource, `last_modified`, and row
count); a changed source replaces the entire local raw file. No history or
content hashes are retained. Loading replaces the raw table; modeling rebuilds
cleaned and output together, never appending. Additions, corrections, and
deletions are reflected in the next successful rebuild, and identical input
produces identical tables. A modeling failure preserves the earlier modeled
tables even if raw was refreshed. See [docs/pipeline.md](docs/pipeline.md).

## At 100x Scale or With Daily Refreshes

I would change the setup based on measured limits and production needs, not
automatically because the dataset became 100x larger.

- **Compute and database:** Measure runtime, peak memory, and query performance.
  First tune queries and increase CPU or memory where useful. Keep DuckDB while
  it finishes within the time available and meets access needs. Move to a shared
  analytical database or warehouse when runtime, memory, or concurrent access
  needs exceed what the local setup can reasonably support.
- **Object storage and Parquet:** Store unchanged raw snapshots in S3 or similar
  object storage, with source metadata and content hashes. Use Parquet for
  cleaned and intermediate data to reduce storage and avoid reading unnecessary
  columns. S3 provides storage; it does not replace a query engine or solve
  compute limits by itself.
- **Daily scheduling:** For a production daily pipeline, use Airflow to schedule
  runs, manage retries and dependencies, and support backfills. Prevent
  overlapping runs. A simple scheduler would still be enough if this remained
  one independent job.
- **Modeling and validation:** Introduce dbt and dbt data tests as the models and
  their dependencies become more complex. This would make reusable checks and
  model documentation easier to maintain. Keep pytest for Python ingestion and
  execution logic.
- **Incremental loads and source updates:** Verify stable keys and how the source
  reports updates before adding incremental loads. Handle corrected and deleted
  records, not just new sales. Reprocess a recent period (a lookback window) if
  the source supports it, and periodically compare all modeled records with the
  full source. If reliable change tracking is unavailable, compare full snapshots
  instead of assuming the source only adds new records.
- **Monitoring:** Track runtime, source freshness when available, row counts,
  parsing failures, exclusions, and quality check results. Alert on failed runs,
  unexpected schema changes, and large changes in counts. A successful download
  alone does not mean the source data is current or correct.
- **Retention and reproducibility:** Define how long raw snapshots, intermediate
  files, and run logs are kept. Use storage lifecycle rules to archive or delete
  older files automatically, while keeping enough history for debugging and
  backfills.

## What I Am Leaving Out and Why

- **Airflow:** A local command is enough for the exercise. A scheduler and its
  supporting services would add setup without a current daily production need.
- **dbt, dbt data tests, and Great Expectations:** The implemented SQL files
  and checks are enough for this small pipeline.
- **S3, a shared warehouse, and distributed processing:** Local storage and
  DuckDB are enough. I did not add cloud infrastructure or more compute without
  evidence that runtime, memory, or access needs require it.
- **Property valuation or causal claims:** Recorded transactions alone do not
  explain why prices change. Property mix and transfer types can affect medians.
- **Incremental loading:** A full rebuild is simpler until source update
  behavior is understood.
- **Raw snapshot history and content hashes:** Only the latest source is needed
  here. Historical audits and exact reproduction of earlier results would
  require a different retention policy.
- **Automatic removal of unusual prices:** A low or high price is not necessarily
  an error. Only the documented, evidence-based sale rules exclude rows.
- **Inflation adjustment and residential-only analysis:** Prices are nominal,
  and property-use information is unavailable. Changing sale mix can change
  medians without the same homes appreciating.
- **CSV export and hosted presentation:** The required output is the validated
  DuckDB table. A read-only analysis notebook presents the results directly;
  exports and hosting are unnecessary for this exercise.
- **Automated HTTP tests:** Ingestion tests cover CSV validation only. Unchanged
  skips, source updates, and HTTP failure recovery were verified by running
  `make ingest`, not by mocked tests.

## Project Setup

Use Python 3.12 and Make. Any Python 3.12 patch release is supported.
Install dependencies and run tests from the repository root:

```sh
make install
make test
```

Make uses `python3.12` by default. If it is not on your PATH, provide the path
to your Python 3.12 interpreter:

```sh
make install PYTHON=/path/to/python3.12
```

`make install` creates `.venv` and installs dependencies with pip. Make commands
use that environment directly, so shell activation is not required.

`make test` runs 12 offline tests using one small synthetic CSV and temporary
DuckDB databases. Tests use plain helpers and assertions, without HTTP mocks,
patched functions, or custom fixture decorators. Invalid inputs are created
inside tests; larger numerical examples are generated by a helper.

Ingestion tests cover CSV validation only. Run `make ingest` to check the real
HTTP path; skipping, downloading source updates, and HTTP failure recovery do
not have automated test coverage. Safe download staging, source-change checks,
timeouts, and retries remain in the production code.

After setup, produce the output with:

```sh
make run
```

`make run` executes ingestion, loading, and transformation in order, stopping
if any stage fails. Loading creates the DuckDB file if it is missing.

| Command | Current behavior |
| --- | --- |
| `make ingest` | Downloads and validates raw data; see [docs/pipeline.md](docs/pipeline.md) |
| `make load` | Loads the existing local CSV into the raw DuckDB table; see [docs/pipeline.md](docs/pipeline.md) |
| `make transform` | Cleans and models loaded raw data, checks results, then commits both modeled tables |
| `make run` | Runs ingestion, loading, and transformation sequentially |
| `make help` | Lists available commands |
| `make clean` | Removes caches from source, tests, and notebooks; keeps data and `.venv` |

No manual downloads are required. Pipeline-specific instructions are in
[docs/pipeline.md](docs/pipeline.md).

## Repository Layout

Python pipeline stages live in `src/`, with SQL transformations grouped in
`src/sql/`. Exploratory notebooks and tests stay in separate directories at the
repository root.

- [src/pipeline.py](src/pipeline.py): Quality checks and transactional modeling entry point.
- [src/sql/clean.sql](src/sql/clean.sql): Row-preserving cleaning and market-sale eligibility.
- [src/sql/model.sql](src/sql/model.sql): Annual medians, changes, and eligible growth ranks.
- [src/ingest.py](src/ingest.py): CKAN download and raw-file validation.
- [src/load.py](src/load.py): Validated, transactional raw DuckDB loading.
- [src/__init__.py](src/__init__.py): Makes `src` a Python package.
- [tests/test_pipeline.py](tests/test_pipeline.py): Cleaning/modeling, failures, updates, reruns, and rollback tests.
- [tests/test_ingest.py](tests/test_ingest.py): Parsed CSV record counting and invalid-input tests.
- [tests/test_load.py](tests/test_load.py): Raw text preservation, repeat loading, replacement, and bad-input tests.
- [tests/data/sales_sample.csv](tests/data/sales_sample.csv): Small synthetic sample of observed data issues.
- [notebooks/01_exploratory_data_analysis.ipynb](notebooks/01_exploratory_data_analysis.ipynb): EDA queries and charts supporting the rules.
- [notebooks/02_market_price_analysis.ipynb](notebooks/02_market_price_analysis.ipynb): Read-only analysis of annual prices, growth ranks, and sample coverage from DuckDB.
- [requirements.txt](requirements.txt): Pinned direct dependencies.
- [Makefile](Makefile): Setup, pipeline, test, and cache cleanup commands.
- [.gitignore](.gitignore): Excludes local data, environments, caches, and secrets.
- [docs/adr-pipeline-design.md](docs/adr-pipeline-design.md): Key decisions and reasons.
- [docs/pipeline.md](docs/pipeline.md): Pipeline operations, ingestion, and loading details.

The data directories are:

- `data/raw/`: Latest unchanged source CSV and separate ingestion metadata.
- `data/duckdb/`: Local database, `property_sales.duckdb`, containing the
  `raw`, `cleaned`, and `output` schemas.

Each directory contains a `.gitkeep` file so it exists after cloning. Only the
placeholders are committed; raw data and DuckDB files stay ignored by Git.

## Exploratory Analysis

[notebooks/01_exploratory_data_analysis.ipynb](notebooks/01_exploratory_data_analysis.ipynb)
explores the raw CSV and documents the findings behind the cleaning and sale
rules. Run `make ingest` first; the notebook is not required for pipeline runs.

## Results Notebook

[notebooks/02_market_price_analysis.ipynb](notebooks/02_market_price_analysis.ipynb)
reads the validated DuckDB output read-only and presents price trends, growth
rankings, and sample coverage. Run `make run` first, then rerun the notebook
after refreshes to update its charts, tables, and conclusions.

## AI Disclosure

I used GitHub Copilot to discuss design choices, generate and refine pipeline
code, SQL, tests, notebooks, and documentation, investigate data issues, and
help verify results and rerun safety.