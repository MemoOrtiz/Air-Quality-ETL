"""Issue #13 (D3): LocalStorage metadata writes always overwrite, like S3Storage.

A same-day re-run must refresh the metadata files. The previous behaviour skipped
the write when the file already existed, which froze a possibly incomplete catalog
until the ingest_date= partition rolled over the next day.
"""
import json

import pytest

from src.ingestion.openaq.storage.local_filesystem import LocalStorage

ZONE = "Test_Zone"
INGEST_DATE = "2026-09-06"
LOCATION_ID = 42

FIRST_RUN = [{"id": 1}]
SECOND_RUN = [{"id": 1}, {"id": 2}]

CASES = [
    pytest.param(
        lambda storage, payload: storage.save_locations_index(ZONE, payload, INGEST_DATE),
        "locations_index.json",
        lambda payload: {"results": payload},
        id="save_locations_index",
    ),
    pytest.param(
        lambda storage, payload: storage.save_sensors_by_location(ZONE, LOCATION_ID, payload, INGEST_DATE),
        f"sensors_by_location/location_id={LOCATION_ID}.json",
        lambda payload: {"results": payload},
        id="save_sensors_by_location",
    ),
    pytest.param(
        lambda storage, payload: storage.save_sensors_index(ZONE, payload, INGEST_DATE),
        "sensors_index.json",
        lambda payload: payload,
        id="save_sensors_index",
    ),
]


@pytest.mark.parametrize("save, relative_path, expected_content", CASES)
def test_rewriting_metadata_the_same_day_keeps_the_second_write(
    tmp_path, save, relative_path, expected_content
):
    storage = LocalStorage(base=str(tmp_path))

    assert save(storage, FIRST_RUN) is True
    assert save(storage, SECOND_RUN) is True, "a re-write must report success, like S3Storage"

    path = tmp_path / f"zone={ZONE}" / "metadata" / f"ingest_date={INGEST_DATE}" / relative_path
    assert json.loads(path.read_text(encoding="utf-8")) == expected_content(SECOND_RUN)
