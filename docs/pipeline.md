# Property Sales Pipeline

## Status and Scope

Ingestion is implemented in [src/ingest.py](../src/ingest.py). It downloads the
complete combined Property Sales Transactions resource, lands its bytes
unchanged, and validates the file before replacing the previous raw copy.
Raw loading is implemented in [src/load.py](../src/load.py), separately from
downloading. [src/pipeline.py](../src/pipeline.py) cleans loaded raw, validates
it, and publishes the annual analytical table transactionally.

The analytical question and implemented `municipality_year_sales` table are
described in the [README](../README.md). Design decisions are recorded in the
single [ADR](adr-pipeline-design.md).

## Source

- Dataset: [Allegheny County Property Sale Transactions](https://data.wprdc.org/dataset/real-estate-sales).
- Combined resource ID: `5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1`.
- CSV: `https://data.wprdc.org/datastore/dump/5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1`.
- Metadata: `https://data.wprdc.org/api/3/action/resource_show?id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1`.
- Schema and total: `https://data.wprdc.org/api/3/action/datastore_search?resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1&limit=0`.

The live download used by the current database (retrieved 2026-11-02, source
last modified 2026-10-30) contained 506,991 records and 26 columns. That
count is an observation, not a hardcoded expectation: each refresh uses the live
datastore total. The expected schema is fixed in `EXPECTED_COLUMNS` in
[src/ingest.py](../src/ingest.py), including `_id`, `PARID`, `SALEDATE`, `PRICE`,
and municipality and transfer fields.

Use the combined resource rather than assembling the filtered per-year
resources. The full CSV avoids pagination across a changing datastore. If the
dump becomes unavailable, fail visibly; paginated ingestion is a possible
future fallback, not implemented automatic recovery.

## Running Ingestion

After project setup, run from the repository root:

```sh
make ingest
```

`make ingest` uses `.venv/bin/python -m src.ingest`. It exits 0 after a validated
download or an unchanged-source skip, and 1 on failure with a message on stderr.
Then run `make load` and `make transform`, or use `make run` for all three stages.
No manual download is needed.

Local files, all ignored by Git:

| File | Purpose |
| --- | --- |
| `data/raw/sales.csv` | Latest complete CSV, never normalized or rewritten |
| `data/raw/sales.meta.json` | Resource ID, dump URL, `last_modified`, parsed row count, and UTC retrieval time |
| `data/raw/sales.csv.tmp` | Download staging; removed on success or a handled failure |
| `data/raw/sales.meta.json.tmp` | Metadata staging; removed on success or a handled failure |

## Ingestion Steps

1. Query resource metadata and datastore schema/count. Reject an unexpected
   schema or a nonpositive record count.
2. Skip only when the CSV exists and saved resource ID, URL, nonempty
   `last_modified`, and row count match the current source. Missing, malformed,
   or unmatched metadata causes a full download. A missing timestamp also
   causes a download rather than a skip.
3. Stream the CSV response to the temporary file in 1 MiB chunks. Use connect
   and read timeouts of 10 and 120 seconds. GET requests retry up to three times
   with backoff for connection failures and statuses 429, 500, 502, 503, and 504.
   An interrupted response body fails the run; rerun to restart the download.
4. Parse the temporary file solely for validation. Do not modify the bytes.
   Python's CSV reader counts records, not physical lines, so quoted newlines
   are handled correctly. UTF-8 BOMs are accepted while preserving raw bytes.
5. Requery metadata and schema/count. Fail if the timestamp or total changed
   during the download, or the schema no longer matches.
6. Prepare the new metadata, remove the old skip marker, and replace the CSV
   with `os.replace`. Then replace the metadata using the same mechanism.
   Temporary files are in the same directory as their destinations.

## Overwrite, Reruns, and Updates

Keep one raw file, not timestamped snapshots. No hashes or historical versions
are retained. An update replaces the entire file, never appends, so new sales,
historical corrections, and deletions are reflected in the next raw copy.
After loading, modeling rebuilds cleaned and output from that snapshot.

WPRDC's documented daily ETL wipes and reloads its datastore. `_id` therefore
must not be treated as a stable sale key across runs. A daily reload can change
`last_modified` even when values are identical; downloading again is acceptable
at this size. The skip rule assumes CKAN updates its timestamp when data changes;
it cannot detect same-count corrections under an unchanged timestamp. Delete
`data/raw/sales.meta.json` and rerun to force a fresh download.

HTTP, parsing, and validation failures leave the previous CSV and metadata
intact. CSV and metadata replacement are individually atomic, not a single
two-file transaction. If publication fails after CSV replacement, the new CSV
is already validated but metadata is absent; the next run downloads again.
Invalidating the old marker before publishing prevents a stale skip after a
crash. A hard kill may leave staging files; the next download overwrites them.

Run only one ingestion process at a time: the fixed staging paths do not support
concurrent writers. The source recheck detects observable refreshes, not an
upstream transactional snapshot guarantee. Without history or hashes, this
project cannot reconstruct old results or detect arbitrary local CSV edits on
a skipped run. Revisit retention and locking for audits or scheduled production.

## Raw DuckDB Loading

Run after ingestion has created the local CSV and metadata:

```sh
make load
```

This runs `.venv/bin/python -m src.load` without HTTP requests. It exits 0 after
committing a validated raw table, or 1 with an error on stderr. It does not run
ingestion automatically. Run `make transform` next to clean and model loaded raw;
it does not download data or reload the CSV.

The database is `data/duckdb/property_sales.duckdb`, and the table is
`raw.property_sales`. Paths are resolved from the repository rather than the
current working directory. The loading function accepts alternate paths for
isolated tests; no extra CLI configuration or dependencies are added.

### Preservation Rules

All 26 source columns are loaded as `VARCHAR`, with their original names and
order. Explicit types avoid numeric and date inference; `force_not_null` on all
columns preserves empty fields as empty strings. Leading zeros, whitespace,
literal `NA`/`NULL` values, quoted commas/newlines, and source duplicates are
retained. No trimming, filtering, deduplication, or casting is performed.

The CSV is still the unchanged-byte source of record. DuckDB contains parsed
text fields, not the CSV's quoting, delimiters, BOM, or line-ending bytes.

### Load Steps and Checks

1. Require the local CSV and JSON ingestion metadata. Metadata must be an
   object identifying the expected resource ID and dump URL, with a positive
   integer `row_count`. A missing source timestamp is acceptable. Missing or
   invalid inputs fail with guidance to run `make ingest`.
2. Reuse ingestion's `validate_csv` to check the exact header, record widths,
   strict Python CSV parsing, and count against metadata before opening DuckDB.
3. Create the database directory if needed, open the connection, and begin a
   transaction. Create schema `raw` if absent and use `CREATE OR REPLACE TABLE`
   to load only the expected source columns from the local CSV.
4. Check the loaded table's ordered column names and `VARCHAR` types, then
   compare `count(*)` with metadata. Commit only after those checks pass.
5. On a load or table-check failure, roll back and close the connection.
   Successful loading prints the committed count and destination table.

**Source-specific parser setting:** The actual dump has a CRLF header followed
by LF record terminators. DuckDB 1.4.2's strict reader rejected this file, despite
Python's strict CSV reader accepting it. DuckDB uses `strict_mode=false` for
this mixed-ending source, but `ignore_errors=false` and `null_padding=false`:
records must not be skipped or padded. Strict Python preflight and loaded-table
schema/count checks remain mandatory. A regression fixture reproduces the mixed
line endings. Full-data verification compared every loaded field against Python
CSV parsing and found no differences across all 506,991 records.

### Reruns and Recovery

Every `make load` rebuilds the raw table, even if ingestion skipped an unchanged
download. There is no separate loading cache, history, or append operation.
Changed local input replaces old records, including reflecting source deletions;
duplicates already present in the CSV remain present in the table.

Input failures leave the database unopened. Failures inside the transaction
preserve the previous raw table; unrelated database tables are never replaced.
On a first attempt, opening DuckDB may create an empty database file even if the
transaction later fails. Only the table is replaced, not the whole database
file, and DuckDB manages its own WAL. Database and WAL files are ignored by Git.
The loader never updates or deletes the source CSV or ingestion metadata.

Run loading and ingestion sequentially, with only one writer at a time. Metadata
identity and count checks do not prove content identity after arbitrary manual
CSV edits; no hashes are added. A committed raw load does not refresh modeled
tables. Run `make transform` afterwards; a failure preserves earlier modeled results
but does not undo the committed raw refresh.

## Quality Checks and Tests

Implemented ingestion checks fail on:

- Changed API schema or CSV header, including unexpected extra columns.
- Empty datastore or invalid reported record count.
- Malformed CSV syntax or records with the wrong number of fields.
- Parsed CSV count different from the current datastore total.
- Detectable source refresh during download.
- HTTP or unsuccessful CKAN responses.

**Dataset-driven check:** Initial source inspection reported 507,120 loader
lines versus 506,991 datastore records. Because those counts differ, validate
download completeness using parsed records and the live datastore total, not
the ETL line count. The difference alone is not proof of duplicates or errors;
its cause still needs investigation.

[tests/test_ingest.py](../tests/test_ingest.py) validates the small synthetic
[sample CSV](../tests/data/sales_sample.csv), including a quoted newline, without
changing its bytes. Bad headers, record counts, and field counts must fail.
These tests do not exercise HTTP. Use `make ingest` for a live check; unchanged
skips, source updates, and HTTP failure recovery have no automated coverage.
The production download safeguards remain unchanged.

[tests/test_load.py](../tests/test_load.py) tests persisted text values, source
byte preservation, BOMs, mixed line endings, duplicate retention, repeat loads,
changed inputs, and unrelated-table preservation. Representative missing-metadata,
header, source-identity, and count failures must leave the previous table intact.
The loader does not make HTTP requests. These tests validate input rejection
before loading; they do not inject a post-load failure.

The 12 offline tests use plain helpers, temporary files/databases, and assertions,
without mocks, patched functions, or custom fixture decorators. They focus on
the assignment's core guarantees and observed issues rather than every metadata
variant or CLI message. Production validation still enforces the checks above.

Live verification loaded 506,991 records with all 26 columns as `VARCHAR`, checked
every field against Python CSV parsing, and compared complete row multisets
after a second load using `EXCEPT ALL` in both directions. Source-file sizes and
modification times remained unchanged.

## End-to-End Pipeline

```sh
make run
```

After `make install`, this executes `make ingest`, `make load`, and
`make transform` sequentially, including when Make is invoked with `-j`.
It stops if any stage fails. Loading creates the database directory and DuckDB
file if missing. An unchanged-source ingestion skip still proceeds to loading
and transformation. Successful earlier stages are not rolled back if a later
stage fails.

## Cleaning and Modeling

```sh
make transform
```

`make transform` invokes `.venv/bin/python -m src.pipeline` without HTTP requests.
It requires an existing database and local ingestion metadata, then checks raw
schema/count against metadata. These checks are not content hashes: always
load after refreshing ingestion (`make run` does this). Failed checks produce a
nonzero exit. A successful run prints exclusion counts, unknown deed references,
flagged recording dates, and per-year ranking coverage.

### Cleaned Layer

[src/sql/clean.sql](../src/sql/clean.sql) builds `cleaned.property_sales`, retaining every
raw row. Needed text is trimmed, blanks become `NULL`, identifiers stay text,
and dates/prices are explicitly parsed. Nonblank dates must match `YYYY-MM-DD`
and be valid calendar dates. Nonblank prices must be ordinary numeric text
with at most two decimal places; invalid casts/precision fail rather than round.

`SALEDATE` determines the year. Blank recording dates stay `NULL`. Pre-2000
recording dates are nulled and flagged; more than one fails and requires review.
The pipeline does not guess a correction for `0212-08-01`.

Retain original deed values as `source_deed_book` and `source_deed_page`.
The natural key is `(parcel_id, source_deed_book, source_deed_page, sale_date)`.
Different placeholders must not collapse in identity checks: this would merge
15 distinct source-key groups. `source_id` is snapshot lineage, not a stable key.
No deduplication occurs.

For grouping only, blank/`0`/`00`/`000` references become `NULL`. Other nonblank
references, including `TR` books, are retained without guessing repairs.
`parcels_on_deed` counts complete real book/page groups across the entire input,
including nonvalid transfers and excluded years. Incomplete references have
`parcels_on_deed = NULL` and `deed_reference_missing = true`, never a shared group.

Roll ward codes up: 101-132 Pittsburgh, 201-202 Clairton, 301-303 Duquesne,
401-412 McKeesport. Other municipalities use stable text-code keys and trimmed
names with doubled apostrophes normalized. Known 175-code membership and
130-municipality coverage are explicit contracts; drift requires review.

`is_market_sale` requires code `0`, price at least $1,000, and either a real
single-row deed or an unknown reference. Unknown references are retained
following EDA but cannot establish single-parcel status. The pipeline prints
their count, recording anomalies, exclusion counts, and annual ranking coverage.
`exclusion_reason` records the first applicable rule; exclusions can overlap.

### Annual Output

[src/sql/model.sql](../src/sql/model.sql) builds `output.municipality_year_sales`.
Defaults are start year 2020, end year before the run date, and sample threshold
30. This snapshot publishes 2020-2025. The Python API accepts `as_of_date`,
`start_year`, and `min_sample` for tests/reuse; Make uses defaults.

Aggregate counts and exact medians. Three-decimal median precision preserves
half-cent averages that DuckDB would truncate at two places. Missing sales
years are absent, not zero. Join only the immediately preceding published year:

```text
dollar change = current median - previous median
percent change = 100 * (current median - previous median) / previous median
```

Gaps yield `NULL` changes. 2020 never compares to unpublished 2019. Guard a
zero denominator. Both adjacent years need 30 sales to rank; small groups
remain visible with `low_sample_flag`. Use `DENSE_RANK` over unrounded percentage
changes within each year; ties share ranks and ineligible rows have no rank.
Negative changes are preserved, not described as increases.

The README lists all columns. Inspect the fastest 2025 changes with:

```sh
.venv/bin/python - <<'PY'
import duckdb
from src.load import DB_PATH
with duckdb.connect(str(DB_PATH), read_only=True) as connection:
      rows = connection.execute('''
            SELECT municipality, sale_count, previous_year_sale_count,
                      median_sale_price, yoy_percent_change, yoy_growth_rank
            FROM output.municipality_year_sales
            WHERE sale_year = 2025 AND ranking_eligible
            ORDER BY yoy_growth_rank, municipality LIMIT 10
      ''').fetchall()
      for row in rows:
            print(row)
PY
```

### Modeled Quality Gates

Named checks fail on:

- Lost/added rows, missing core fields, duplicate natural keys, invalid date
   or price parsing, negative prices, future sales, or sales before 2012.
- Surviving pre-2000 recording dates or more than one nulled anomaly.
- Unknown codes, inconsistent municipality names, or unexpected coverage.
- Grouped missing references, surviving placeholders, invalid market-sale
   eligibility, or multiple market rows per real deed.
- Empty output, duplicate keys, wrong years, null/nonpositive sale counts, or
   total output sale counts that differ from qualifying cleaned records in the
   publication window.

Exact medians, adjacent-year changes, sample eligibility, and growth ranks are
tested with known expected results instead of recomputed by runtime checks.
Conservation checks verify the total number of sales, not every annual aggregate.

The 47 observed sale codes are an approved membership set: not every code must
appear in each refresh. Historical counts and sale shares are diagnostics, not
arbitrary hard failure bands. The inferred coding break is not independently
confirmed with the assessor.

### Publication and Recovery

One explicit transaction validates raw, replaces/checks cleaned, replaces/checks
output, then commits. Any precommit exception rolls back both modeled tables.
Raw, CSV/metadata, and unrelated tables are never changed. Initial failure
publishes no modeled tables; a missing database is not created.

Full rebuilds, not appends/upserts, make identical input repeatable and reflect
reloaded additions, corrections, and deletions. A modeled failure cannot undo
an already committed raw refresh; previous modeled results remain from the
earlier successful run. Fix the input/contract and rerun. Use one writer and
sequential stages.

[tests/test_pipeline.py](../tests/test_pipeline.py) covers exact medians,
Pittsburgh ward roll-up, filters, original placeholder identities,
cross-year/code deeds, thresholds, gaps/baseline/partial years, ties/declines,
malformed inputs, source replacement, reruns, output integrity checks, and
rollback after cleaned or output checks fail. Plain helpers create isolated
databases; their smaller municipality-count contract does not alter the
production default. No modeling test requires network access.

### Read-Only Results Analysis

Open [02_market_price_analysis.ipynb](../notebooks/02_market_price_analysis.ipynb)
after `make run`, select the project's `.venv` kernel, and run all cells.
It finds the project database from the current directory or its parents and
requires that the database and output table already exist. The connection is
read-only and closes after loading; the notebook never reruns ingestion or
modeling, modifies data, or exports additional tables.

Charts show selected municipality price trends and the latest year's top-ten
growth ranks. Tables show annual sample coverage, every annual growth leader,
the endpoint direction across the period, and all municipality-year medians and
counts. Written summaries are generated from the current output.
Missing observations remain missing, low samples are visible, and ranks are
read from the validated model rather than recalculated from rounded values.
The notebook does not repeat model checks; SQL checks and pytest own validation.

### Interpretation and Remaining Work

Prices are nominal consideration for selected sales, not valuations or
exclusively residential prices. Property mix affects medians. Only 59-64
municipalities qualify per 2021-2025 ranking in this snapshot. Completed years
can still change because of recording lag and corrections; excluding the
partial year does not establish finality.

The required output is the DuckDB table, presented in the results notebook.
CSV export, independent assessor confirmation, inflation adjustment, snapshot retention,
and production scheduling remain deliberately outside this implementation.
