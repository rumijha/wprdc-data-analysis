import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


RESOURCE_ID = "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
API_URL = "https://data.wprdc.org/api/3/action/"
DUMP_URL = f"https://data.wprdc.org/datastore/dump/{RESOURCE_ID}"
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
TIMEOUT = (10, 120)
EXPECTED_COLUMNS = (
    "_id", "PARID", "FULL_ADDRESS", "PROPERTYHOUSENUM", "PROPERTYFRACTION",
    "PROPERTYADDRESSDIR", "PROPERTYADDRESSSTREET", "PROPERTYADDRESSSUF",
    "PROPERTYADDRESSUNITDESC", "PROPERTYUNITNO", "PROPERTYCITY",
    "PROPERTYSTATE",
    "PROPERTYZIP", "SCHOOLCODE", "SCHOOLDESC", "MUNICODE", "MUNIDESC",
    "RECORDDATE", "SALEDATE", "PRICE", "DEEDBOOK", "DEEDPAGE", "SALECODE",
    "SALEDESC", "INSTRTYP", "INSTRTYPDESC",
)


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_result(session: requests.Session, action: str, params: dict) -> dict:
    with session.get(
        API_URL + action, params=params, timeout=TIMEOUT,
    ) as response:
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise ValueError(f"CKAN {action} failed")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError(f"CKAN {action} returned an invalid result")
    return result


def source_state(session: requests.Session) -> tuple[str | None, int]:
    resource = get_result(session, "resource_show", {"id": RESOURCE_ID})
    modified = resource.get("last_modified")
    if modified is not None and not isinstance(modified, str):
        raise ValueError("Invalid source last_modified")
    datastore = get_result(
        session, "datastore_search", {"resource_id": RESOURCE_ID, "limit": 0},
    )
    fields = datastore.get("fields", [])
    if tuple(field["id"] for field in fields) != EXPECTED_COLUMNS:
        raise ValueError("Source schema differs from expected columns")
    total = datastore.get("total")
    if type(total) is not int or total <= 0:
        raise ValueError("Source row count must be a positive integer")
    return modified, total


def validate_csv(path: Path, expected_total: int) -> None:
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, strict=True)
        if tuple(next(reader, ())) != EXPECTED_COLUMNS:
            raise ValueError("CSV header differs from expected columns")
        total = 0
        for total, row in enumerate(reader, start=1):
            if len(row) != len(EXPECTED_COLUMNS):
                raise ValueError(
                    f"CSV record {total} has an invalid field count"
                )
    if total != expected_total:
        raise ValueError(
            f"CSV row count {total} differs from source total {expected_total}"
        )


def ingest(session: requests.Session, raw_dir: Path = RAW_DIR) -> bool:
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "sales.csv"
    meta_path = raw_dir / "sales.meta.json"
    csv_tmp = raw_dir / "sales.csv.tmp"
    meta_tmp = raw_dir / "sales.meta.json.tmp"
    try:
        modified, total = source_state(session)
        try:
            saved = json.loads(meta_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            saved = {}
        if (
            isinstance(saved, dict) and modified and csv_path.is_file()
            and saved.get("resource_id") == RESOURCE_ID
            and saved.get("url") == DUMP_URL
            and saved.get("last_modified") == modified
            and saved.get("row_count") == total
        ):
            print("Source unchanged; keeping sales.csv")
            return False
        with session.get(DUMP_URL, stream=True, timeout=TIMEOUT) as response:
            response.raise_for_status()
            with csv_tmp.open("wb") as destination:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    destination.write(chunk)
        validate_csv(csv_tmp, total)
        if source_state(session) != (modified, total):
            raise ValueError("Source changed during download; retry ingestion")
        metadata = {
            "resource_id": RESOURCE_ID, "url": DUMP_URL,
            "last_modified": modified, "row_count": total,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }
        meta_tmp.write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8",
        )
        meta_path.unlink(missing_ok=True)
        os.replace(csv_tmp, csv_path)
        os.replace(meta_tmp, meta_path)
        print(f"Downloaded and validated {total:,} records to {csv_path}")
        return True
    finally:
        csv_tmp.unlink(missing_ok=True)
        meta_tmp.unlink(missing_ok=True)


def main() -> int:
    try:
        with create_session() as session:
            ingest(session)
    except (
        requests.RequestException, OSError, ValueError, KeyError, TypeError,
        csv.Error,
    ) as error:
        print(f"Ingestion failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
