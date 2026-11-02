from pathlib import Path

import pytest

from src.ingest import validate_csv


SAMPLE_CSV = Path(__file__).parent / "data" / "sales_sample.csv"


def test_csv_counts_records_not_lines():
    before = SAMPLE_CSV.read_bytes()
    validate_csv(SAMPLE_CSV, expected_total=10)
    assert len(before.splitlines()) > 11
    assert SAMPLE_CSV.read_bytes() == before


def test_csv_rejects_bad_header_count_and_record(tmp_path):
    with pytest.raises(ValueError, match="row count"):
        validate_csv(SAMPLE_CSV, expected_total=11)

    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(SAMPLE_CSV.read_text().replace("PARID", "wrong", 1))
    with pytest.raises(ValueError, match="header"):
        validate_csv(bad_csv, expected_total=10)

    bad_csv.write_text(SAMPLE_CSV.read_text() + "incomplete,record\n")
    with pytest.raises(ValueError, match="field count"):
        validate_csv(bad_csv, expected_total=11)
