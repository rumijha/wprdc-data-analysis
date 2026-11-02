import csv
import json
from pathlib import Path

import duckdb
import pytest

from src import load
from src.ingest import DUMP_URL, EXPECTED_COLUMNS, RESOURCE_ID


def write_raw(raw_dir: Path, rows: list[list[str]]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    with (raw_dir / "sales.csv").open(
        "w", encoding="utf-8", newline="",
    ) as destination:
        writer = csv.writer(destination)
        writer.writerow(EXPECTED_COLUMNS)
        writer.writerows(rows)
    (raw_dir / "sales.meta.json").write_text(
        json.dumps({
            "resource_id": RESOURCE_ID, "url": DUMP_URL,
            "row_count": len(rows), "last_modified": None,
        }),
        encoding="utf-8",
    )


def read_rows(db_path: Path) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return connection.execute(
            "SELECT * FROM raw.property_sales ORDER BY ALL"
        ).fetchall()


def test_load_preserves_text_and_source_bytes(tmp_path):
    raw_dir = tmp_path / "raw"
    db_path = tmp_path / "sales.duckdb"
    sample = Path(__file__).parent / "data" / "sales_sample.csv"
    with sample.open(newline="") as source:
        expected_rows = list(csv.reader(source))[1:]
    write_raw(raw_dir, expected_rows)
    csv_path = raw_dir / "sales.csv"
    content = sample.read_bytes().replace(b"\r\n", b"\n")
    csv_path.write_bytes(b"\xef\xbb\xbf" + content.replace(b"\n", b"\r\n", 1))
    before = {path.name: path.read_bytes() for path in raw_dir.iterdir()}
    assert load.load_raw(raw_dir, db_path) == 10
    assert read_rows(db_path) == sorted(tuple(row) for row in expected_rows)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        columns = connection.execute(
            "DESCRIBE raw.property_sales"
        ).fetchall()
        assert [(row[0], row[1]) for row in columns] == [
            (name, "VARCHAR") for name in EXPECTED_COLUMNS
        ]
    assert before == {
        path.name: path.read_bytes() for path in raw_dir.iterdir()
    }


def test_rerun_update_and_bad_input_preserve_database(tmp_path):
    raw_dir = tmp_path / "raw"
    db_path = tmp_path / "sales.duckdb"
    write_raw(raw_dir, [["original"] * len(EXPECTED_COLUMNS)])
    load.load_raw(raw_dir, db_path)
    original_rows = read_rows(db_path)
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("CREATE TABLE unrelated AS SELECT 42 AS value")
    load.load_raw(raw_dir, db_path)
    assert read_rows(db_path) == original_rows
    updated = [["changed"] * len(EXPECTED_COLUMNS)] * 2
    write_raw(raw_dir, updated)
    assert load.load_raw(raw_dir, db_path) == 2
    assert read_rows(db_path) == [tuple(row) for row in updated]
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT value FROM unrelated"
        ).fetchone() == (42,)
    previous = read_rows(db_path)
    for failure in (
        "missing_metadata", "wrong_header", "wrong_count", "wrong_source", "invalid_count",
    ):
        write_raw(raw_dir, updated)
        meta_path = raw_dir / "sales.meta.json"
        if failure == "missing_metadata":
            meta_path.unlink()
        elif failure == "wrong_header":
            (raw_dir / "sales.csv").write_text("wrong\nvalue\n")
        else:
            metadata = json.loads(meta_path.read_text())
            if failure == "wrong_source":
                metadata["resource_id"] = "another-source"
            elif failure == "invalid_count":
                metadata["row_count"] = -1
            else:
                metadata["row_count"] = 3
            meta_path.write_text(json.dumps(metadata))
        with pytest.raises(ValueError, match="Run make ingest"):
            load.load_raw(raw_dir, db_path)
        assert read_rows(db_path) == previous
