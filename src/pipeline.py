import sys
from datetime import date
from pathlib import Path

import duckdb

from src.ingest import RAW_DIR
from src.load import DB_PATH, read_row_count, validate_loaded


SQL_DIR = Path(__file__).resolve().parent / "sql"
KNOWN_SALE_CODES = frozenset({
    "0", "0X", "1", "13", "14", "16", "19", "2", "21", "27",
    "28", "29", "3", "32", "33", "34", "35", "36", "37", "4",
    "5", "50", "6", "8", "9", "99", "AA", "BK", "DT", "GV",
    "H", "N", "ND", "PA", "PV", "PX", "S", "U", "UR", "UX",
    "V", "VB", "VH", "X", "XX", "YY", "Z",
})
KNOWN_MUNICIPALITY_CODES = frozenset(
    str(code) for code in (
        *range(101, 133), *range(201, 203), *range(301, 304),
        *range(401, 413), *range(801, 885),
        901, 902, 905, 906, 907, 908, 909, 910, 911, 913, 914, 915,
        916, 917, 919, 920, 921, 923, 925, 926, 927, 928, 929, 930,
        931, 932, 934, 935, 937, 938, 939, 940, 941, 944, 945, 946,
        947, 948, 949, 950, 952, 953,
    )
)


def require_zero(
    connection: duckdb.DuckDBPyConnection,
    name: str,
    query: str,
    parameters: dict | None = None,
) -> None:
    result = connection.execute(query, parameters or {}).fetchone()
    if result is None or result[0] != 0:
        count = result[0] if result else "unknown"
        raise ValueError(
            f"Data quality check '{name}' failed: {count} violation(s)"
        )


def validate_cleaned(
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    expected_municipalities: int = 130,
) -> None:
    checks = {
        "row conservation": """
            SELECT ABS((SELECT COUNT(*) FROM raw.property_sales)
                - (SELECT COUNT(*) FROM cleaned.property_sales))
        """,
        "required fields": """
            SELECT COUNT(*) FROM cleaned.property_sales
            WHERE parcel_id IS NULL OR municipality_code IS NULL
                OR municipality_description IS NULL OR sale_code IS NULL
                OR sale_date_text IS NULL
        """,
        "strict date parsing": """
            SELECT COUNT(*) FROM cleaned.property_sales
            WHERE sale_date_parse_invalid OR record_date_parse_invalid
        """,
        "price parsing": """
            SELECT COUNT(*) FROM cleaned.property_sales
            WHERE price_parse_invalid
        """,
        "nonnegative prices": """
            SELECT COUNT(*) FROM cleaned.property_sales WHERE price < 0
        """,
        "natural key uniqueness": """
            SELECT COUNT(*) FROM (
                SELECT parcel_id, source_deed_book, source_deed_page, sale_date
                FROM cleaned.property_sales GROUP BY ALL HAVING COUNT(*) > 1
            )
        """,
        "record date anomaly handling": """
            SELECT COUNT(*) FROM cleaned.property_sales
            WHERE record_date < DATE '2000-01-01'
                OR (record_date_invalid AND record_date IS NOT NULL)
        """,
        "record date anomaly budget": """
            SELECT GREATEST(COUNT(*) - 1, 0)
            FROM cleaned.property_sales WHERE record_date_invalid
        """,
        "municipality mapping consistency": """
            SELECT COUNT(*) FROM (
                SELECT municipality_key FROM cleaned.property_sales
                GROUP BY municipality_key
                HAVING COUNT(DISTINCT municipality) != 1
            )
        """,
        "missing deed references never grouped": """
            SELECT COUNT(*) FROM cleaned.property_sales
            WHERE (deed_reference_missing AND parcels_on_deed IS NOT NULL)
                OR (NOT deed_reference_missing AND
                    (parcels_on_deed IS NULL OR parcels_on_deed < 1))
                OR deed_book IN ('', '0', '00', '000')
                OR deed_page IN ('', '0', '00', '000')
        """,
        "market eligibility": """
            SELECT COUNT(*) FROM cleaned.property_sales
            WHERE is_market_sale AND (
                sale_code IS DISTINCT FROM '0' OR price IS NULL OR price < 1000
                OR parcels_on_deed > 1
            )
        """,
        "one market sale per real deed": """
            SELECT COUNT(*) FROM (
                SELECT deed_book, deed_page FROM cleaned.property_sales
                WHERE is_market_sale AND NOT deed_reference_missing
                GROUP BY ALL HAVING COUNT(*) > 1
            )
        """,
    }
    for name, query in checks.items():
        require_zero(connection, name, query)
    require_zero(connection, "sale date range", """
        SELECT COUNT(*) FROM cleaned.property_sales
        WHERE sale_date < DATE '2012-01-01' OR sale_date > $as_of_date
    """, {"as_of_date": as_of_date})
    require_zero(connection, "known sale codes", """
        SELECT COUNT(*) FROM cleaned.property_sales
        WHERE sale_code NOT IN (SELECT UNNEST($codes))
    """, {"codes": sorted(KNOWN_SALE_CODES)})
    require_zero(connection, "known municipality codes", """
        SELECT COUNT(*) FROM cleaned.property_sales
        WHERE municipality_code NOT IN (SELECT UNNEST($codes))
    """, {"codes": sorted(KNOWN_MUNICIPALITY_CODES)})
    require_zero(connection, "municipality coverage", """
        SELECT ABS(COUNT(DISTINCT municipality_key) - $expected)
        FROM cleaned.property_sales
    """, {"expected": expected_municipalities})


def validate_output(
    connection: duckdb.DuckDBPyConnection,
    start_year: int,
    end_year: int,
) -> None:
    require_zero(connection, "output keys", """
        SELECT COUNT(*) FROM (
            SELECT municipality_key, sale_year
            FROM output.municipality_year_sales
            GROUP BY ALL HAVING COUNT(*) > 1
        )
    """)
    require_zero(connection, "output nonempty", """
        SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
        FROM output.municipality_year_sales
    """)
    require_zero(connection, "output year range", """
        SELECT COUNT(*) FROM output.municipality_year_sales
        WHERE sale_year NOT BETWEEN $start_year AND $end_year
    """, {"start_year": start_year, "end_year": end_year})
    require_zero(connection, "output positive counts", """
        SELECT COUNT(*) FROM output.municipality_year_sales
        WHERE sale_count IS NULL OR sale_count <= 0
    """)
    require_zero(connection, "output sale conservation", """
        SELECT ABS(
            (SELECT COUNT(*) FROM cleaned.property_sales
             WHERE is_market_sale AND sale_year BETWEEN $start_year AND $end_year)
            - (SELECT COALESCE(SUM(sale_count), 0)
               FROM output.municipality_year_sales)
        )
    """, {"start_year": start_year, "end_year": end_year})


def run_pipeline(
    db_path: Path = DB_PATH,
    raw_dir: Path = RAW_DIR,
    *,
    as_of_date: date | None = None,
    start_year: int = 2020,
    min_sample: int = 30,
    expected_municipalities: int = 130,
) -> int:
    as_of_date = as_of_date or date.today()
    end_year = as_of_date.year - 1
    if not 2012 <= start_year <= end_year or min_sample < 1:
        raise ValueError("Invalid start year or minimum sample")
    if not db_path.is_file():
        raise ValueError(
            "Loaded database is missing. Run make ingest then make load."
        )
    expected_total = read_row_count(raw_dir)
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            validate_loaded(connection, expected_total)
            connection.execute(
                (SQL_DIR / "clean.sql").read_text(encoding="utf-8")
            )
            validate_cleaned(connection, as_of_date, expected_municipalities)
            connection.execute(
                (SQL_DIR / "model.sql").read_text(encoding="utf-8"),
                {
                    "start_year": start_year, "end_year": end_year,
                    "min_sample": min_sample,
                },
            )
            validate_output(connection, start_year, end_year)
            output_result = connection.execute(
                "SELECT COUNT(*) FROM output.municipality_year_sales"
            ).fetchone()
            if output_result is None:
                raise ValueError("Missing output count result")
            output_total = output_result[0]
            diagnostics = connection.execute("""
                SELECT COUNT(*) FILTER (WHERE is_market_sale),
                    COUNT(*) FILTER (
                        WHERE is_market_sale AND deed_reference_missing
                    ),
                    COUNT(*) FILTER (WHERE record_date_invalid)
                FROM cleaned.property_sales
            """).fetchone()
            if diagnostics is None:
                raise ValueError("Missing cleaning diagnostics result")
            coverage = connection.execute("""
                SELECT sale_year, SUM(sale_count), COUNT(*),
                    COUNT(*) FILTER (WHERE ranking_eligible)
                FROM output.municipality_year_sales
                GROUP BY sale_year ORDER BY sale_year
            """).fetchall()
            exclusions = connection.execute("""
                SELECT exclusion_reason, COUNT(*) FROM cleaned.property_sales
                WHERE NOT is_market_sale
                GROUP BY exclusion_reason ORDER BY exclusion_reason
            """).fetchall()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    print(
        f"Cleaned {expected_total:,} rows; published {output_total:,} "
        f"municipality-years ({start_year}-{end_year})."
    )
    print(
        f"All-year market sample: {diagnostics[0]:,}; "
        f"unknown deed refs: {diagnostics[1]:,}; "
        f"flagged record dates: {diagnostics[2]}."
    )
    for reason, count in exclusions:
        print(f"Excluded ({reason}): {count:,}")
    for year, sales, municipalities, ranked in coverage:
        print(
            f"{year}: {sales:,} sales, {municipalities} municipalities, "
            f"{ranked} eligible for growth ranking."
        )
    return output_total


def main() -> int:
    try:
        run_pipeline()
    except (OSError, ValueError, duckdb.Error) as error:
        print(
            f"Pipeline failed: {error}. "
            "Previous modeled tables, if any, remain unchanged.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
