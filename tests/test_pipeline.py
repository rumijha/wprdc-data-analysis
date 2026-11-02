import csv
from datetime import date
from decimal import Decimal
import json
from pathlib import Path

import duckdb
import pytest

from src import pipeline
from src.ingest import DUMP_URL, EXPECTED_COLUMNS, RESOURCE_ID


def make_db(tmp_path, rows):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(exist_ok=True)
    (raw_dir / "sales.meta.json").write_text(json.dumps({
        "resource_id": RESOURCE_ID, "url": DUMP_URL, "row_count": len(rows),
    }))
    db_path = tmp_path / "sales.duckdb"
    columns = ", ".join(f'"{name}" VARCHAR' for name in EXPECTED_COLUMNS)
    placeholders = ", ".join("?" for name in EXPECTED_COLUMNS)
    values = []
    for index, changes in enumerate(rows, start=1):
        row = dict.fromkeys(EXPECTED_COLUMNS, "")
        row.update({
            "_id": str(index), "PARID": f"{index:016d}",
            "MUNICODE": "801", "MUNIDESC": "Test Borough",
            "SALECODE": "0", "SALEDATE": "2020-06-01",
            "RECORDDATE": "2020-06-07", "PRICE": "100000.00",
            "DEEDBOOK": str(10000 + index), "DEEDPAGE": "1",
        })
        row.update(changes)
        values.append([row[name] for name in EXPECTED_COLUMNS])
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
        connection.execute(f"CREATE OR REPLACE TABLE raw.property_sales ({columns})")
        connection.executemany(
            f"INSERT INTO raw.property_sales VALUES ({placeholders})", values,
        )
    return db_path, raw_dir


def run_model(db_path, raw_dir, municipalities=1):
    return pipeline.run_pipeline(
        db_path, raw_dir, as_of_date=date(2026, 11, 1),
        expected_municipalities=municipalities,
    )


def annual_rows(code, year, count, price):
    return [{
        "MUNICODE": code, "MUNIDESC": f"Municipality {code}",
        "SALEDATE": f"{year}-06-01", "PRICE": str(price),
    }] * count


def published_rows(db_path):
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return [
            connection.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            for table in ("cleaned.property_sales", "output.municipality_year_sales")
        ]


def test_sample_cleaning_and_market_exclusions(tmp_path):
    sample = Path(__file__).parent / "data" / "sales_sample.csv"
    with sample.open(newline="") as source:
        rows = list(csv.DictReader(source))
    db_path, raw_dir = make_db(tmp_path, rows)
    assert run_model(db_path, raw_dir, municipalities=2) == 2
    with duckdb.connect(str(db_path), read_only=True) as connection:
        actual = connection.execute("""
            SELECT municipality, parcels_on_deed, exclusion_reason,
                record_date, record_date_invalid
            FROM cleaned.property_sales ORDER BY source_id::INTEGER
        """).fetchall()
        assert actual[0][:3] == ("Pittsburgh", 1, None)
        assert actual[1][:3] == ("Pittsburgh", 1, None)
        assert actual[2][:3] == ("O'Hara", None, None)
        assert actual[3][:3] == ("O'Hara", None, None)
        assert actual[4][3:] == (None, True)
        assert actual[5][2] == "not_valid_sale_code"
        assert actual[6][2] == "below_price_floor"
        assert actual[7][1:3] == (2, "multi_parcel_deed")
        assert actual[8][1:3] == (2, "not_valid_sale_code")
        assert actual[9][2:] == ("missing_price", None, False)
        assert connection.execute("""
            SELECT source_deed_book, parcels_on_deed
            FROM cleaned.property_sales WHERE parcel_id = '00003'
            ORDER BY source_deed_book
        """).fetchall() == [("0", None), ("00", None)]
        assert connection.execute("""
            SELECT SUM(sale_count) FROM output.municipality_year_sales
        """).fetchone() == (5,)


def test_exact_medians_and_year_over_year_change(tmp_path):
    rows = annual_rows("801", 2020, 15, "100000.00")
    rows += annual_rows("801", 2020, 15, "100000.01")
    rows += annual_rows("801", 2021, 15, "150000.00")
    rows += annual_rows("801", 2021, 15, "150000.01")
    db_path, raw_dir = make_db(tmp_path, rows)
    run_model(db_path, raw_dir)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        baseline, current = connection.execute("""
            SELECT median_sale_price, sale_count, previous_year_sale_count,
                yoy_price_change, yoy_percent_change, ranking_eligible, yoy_growth_rank
            FROM output.municipality_year_sales ORDER BY sale_year
        """).fetchall()
    assert baseline == (Decimal("100000.005"), 30, None, None, None, False, None)
    assert current[:4] == (Decimal("150000.005"), 30, 30, Decimal("50000.000"))
    assert current[4] == pytest.approx(100 * 50000 / 100000.005)
    assert current[5:] == (True, 1)


def test_ranks_ties_and_sample_thresholds(tmp_path):
    rows = []
    for code, previous, current, previous_count, current_count in [
        ("801", 100000, 150000, 30, 30),
        ("802", 200000, 300000, 30, 30),
        ("803", 100000, 80000, 30, 30),
        ("804", 100000, 150002, 30, 30),
        ("805", 100000, 150000, 29, 30),
        ("806", 100000, 150000, 30, 29),
    ]:
        rows += annual_rows(code, 2020, previous_count, previous)
        rows += annual_rows(code, 2021, current_count, current)
    db_path, raw_dir = make_db(tmp_path, rows)
    run_model(db_path, raw_dir, municipalities=6)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        actual = connection.execute("""
            SELECT municipality_key, yoy_percent_change, low_sample_flag,
                ranking_eligible, yoy_growth_rank
            FROM output.municipality_year_sales
            WHERE sale_year = 2021 ORDER BY municipality_key
        """).fetchall()
    assert actual == [
        ("municipality:801", 50.0, False, True, 2),
        ("municipality:802", 50.0, False, True, 2),
        ("municipality:803", -20.0, False, True, 3),
        ("municipality:804", 50.002, False, True, 1),
        ("municipality:805", 50.0, False, False, None),
        ("municipality:806", 50.0, True, False, None),
    ]


def test_year_gaps_and_partial_year_are_not_compared(tmp_path):
    rows = []
    for year in (2019, 2020, 2022, 2026):
        rows += annual_rows("801", year, 30, year * 100)
    db_path, raw_dir = make_db(tmp_path, rows)
    run_model(db_path, raw_dir)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert connection.execute("""
            SELECT sale_year, previous_year_sale_count, yoy_percent_change, yoy_growth_rank
            FROM output.municipality_year_sales ORDER BY sale_year
        """).fetchall() == [(2020, None, None, None), (2022, None, None, None)]


def test_reruns_and_source_replacement(tmp_path):
    db_path, raw_dir = make_db(tmp_path, [{}, {"SALEDATE": "2021-06-01"}])
    run_model(db_path, raw_dir)
    before = published_rows(db_path)
    run_model(db_path, raw_dir)
    assert published_rows(db_path) == before
    make_db(tmp_path, [{"PRICE": "200000", "SALEDATE": "2022-06-01"}])
    run_model(db_path, raw_dir)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert connection.execute("""
            SELECT sale_year, sale_count, median_sale_price
            FROM output.municipality_year_sales
        """).fetchall() == [(2022, 1, Decimal("200000"))]
        assert connection.execute("SELECT COUNT(*) FROM cleaned.property_sales").fetchone() == (1,)


def test_bad_data_fails_named_checks_without_publication(tmp_path):
    for changes, check in [
        ({"PARID": ""}, "required fields"),
        ({"SALEDATE": "2020-02-30"}, "strict date parsing"),
        ({"PRICE": "bad"}, "price parsing"),
        ({"PRICE": "1000.001"}, "price parsing"),
        ({"PRICE": "-1"}, "nonnegative prices"),
        ({"SALECODE": "NEW"}, "known sale codes"),
        ({"MUNICODE": "9999"}, "known municipality codes"),
        ({"PARID": "0000000000000001", "DEEDBOOK": "10001"}, "natural key uniqueness"),
    ]:
        db_path, raw_dir = make_db(tmp_path, [{}, changes])
        with pytest.raises(ValueError, match=check):
            run_model(db_path, raw_dir)
        with duckdb.connect(str(db_path), read_only=True) as connection:
            assert connection.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema IN ('cleaned', 'output')
            """).fetchone() == (0,)


def test_record_date_budget_and_failed_rebuild_preserve_publication(tmp_path):
    db_path, raw_dir = make_db(tmp_path, [{"RECORDDATE": "0212-08-01"}, {}])
    run_model(db_path, raw_dir)
    before = published_rows(db_path)
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("UPDATE raw.property_sales SET RECORDDATE = '0212-08-01'")
    with pytest.raises(ValueError, match="record date anomaly budget"):
        run_model(db_path, raw_dir)
    assert published_rows(db_path) == before
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("UPDATE raw.property_sales SET RECORDDATE = '2020-06-07', SALECODE = '3'")
    with pytest.raises(ValueError, match="output nonempty"):
        run_model(db_path, raw_dir)
    assert published_rows(db_path) == before


def test_output_integrity_checks_reject_bad_results(tmp_path):
    db_path, raw_dir = make_db(tmp_path, [{}])
    for mutation, check in [
        ("DELETE FROM output.municipality_year_sales", "output nonempty"),
        ("INSERT INTO output.municipality_year_sales SELECT * FROM output.municipality_year_sales", "output keys"),
        ("UPDATE output.municipality_year_sales SET sale_year = 2026", "output year range"),
        ("UPDATE output.municipality_year_sales SET sale_count = 0", "output positive counts"),
        ("UPDATE output.municipality_year_sales SET sale_count = 2", "output sale conservation"),
    ]:
        run_model(db_path, raw_dir)
        with duckdb.connect(str(db_path)) as connection:
            connection.execute(mutation)
            with pytest.raises(ValueError, match=check):
                pipeline.validate_output(connection, 2020, 2025)
