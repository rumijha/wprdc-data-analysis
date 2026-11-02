import csv
import json
from pathlib import Path
import sys

import duckdb

from src.ingest import (
    DUMP_URL, EXPECTED_COLUMNS, RAW_DIR, RESOURCE_ID, validate_csv,
)


DB_PATH = RAW_DIR.parent / "duckdb" / "property_sales.duckdb"


def read_row_count(raw_dir: Path) -> int:
    metadata = json.loads(
        (raw_dir / "sales.meta.json").read_text(encoding="utf-8")
    )
    if not isinstance(metadata, dict):
        raise ValueError("Ingestion metadata must be an object")
    if (
        metadata.get("resource_id") != RESOURCE_ID
        or metadata.get("url") != DUMP_URL
    ):
        raise ValueError("Ingestion metadata identifies a different source")
    total = metadata.get("row_count")
    if type(total) is not int or total <= 0:
        raise ValueError("Metadata row_count must be a positive integer")
    return total


def validate_loaded(
    connection: duckdb.DuckDBPyConnection, expected_total: int,
) -> int:
    columns = connection.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = current_database()
          AND table_schema = 'raw' AND table_name = 'property_sales'
        ORDER BY ordinal_position
        """
    ).fetchall()
    if columns != [(name, "VARCHAR") for name in EXPECTED_COLUMNS]:
        raise ValueError(
            "Loaded table schema differs from expected text columns"
        )
    result = connection.execute(
        "SELECT count(*) FROM raw.property_sales"
    ).fetchone()
    if result is None or result[0] != expected_total:
        actual_total = result[0] if result else None
        raise ValueError(
            f"Loaded row count {actual_total} differs from metadata "
            f"total {expected_total}"
        )
    return result[0]


def load_raw(raw_dir: Path = RAW_DIR, db_path: Path = DB_PATH) -> int:
    csv_path = raw_dir / "sales.csv"
    try:
        expected_total = read_row_count(raw_dir)
        validate_csv(csv_path, expected_total)
    except (OSError, ValueError, csv.Error) as error:
        raise ValueError(
            f"Invalid raw input: {error}. Run make ingest before make load."
        ) from error

    db_path.parent.mkdir(parents=True, exist_ok=True)
    column_types = {name: "VARCHAR" for name in EXPECTED_COLUMNS}
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
            connection.execute(
                """
                CREATE OR REPLACE TABLE raw.property_sales AS
                SELECT *
                FROM read_csv(
                    ?, columns = ?, force_not_null = ?,
                    auto_detect = false, header = true, delim = ',',
                    quote = '"', escape = '"', encoding = 'utf-8',
                    strict_mode = false, ignore_errors = false,
                    null_padding = false, normalize_names = false
                )
                """,
                [str(csv_path), column_types, list(EXPECTED_COLUMNS)],
            )
            total = validate_loaded(connection, expected_total)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    print(f"Loaded {total:,} records into {db_path}: raw.property_sales")
    return total


def main() -> int:
    try:
        load_raw()
    except (OSError, ValueError, csv.Error, duckdb.Error) as error:
        print(f"Loading failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
