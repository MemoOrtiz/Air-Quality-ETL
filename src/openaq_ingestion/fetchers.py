# src/openaq_ingestion/fetchers.py
import os, json
from .api import get
from .config import api_base, PAGE_LIMIT_DEFAULT
from .utils import build_out_folder

def fetch_locations_bbox(bbox: tuple, limit=PAGE_LIMIT_DEFAULT):
    lonW, latS, lonE, latN = bbox
    page, results = 1, []
    while True:
        params = {"bbox": f"{lonW},{latS},{lonE},{latN}", "limit": limit, "page": page}
        r = get(f"{api_base()}/locations", params=params)
        chunk = r.json().get("results", [])
        results.extend(chunk)
        if len(chunk) < limit: break
        page += 1
    return results

def fetch_sensors_for_location(location_id: int, limit=PAGE_LIMIT_DEFAULT):
    page, sensors = 1, []
    while True:
        r = get(f"{api_base()}/locations/{location_id}/sensors", params={"limit": limit, "page": page})
        chunk = r.json().get("results", [])
        sensors.extend(chunk)
        if len(chunk) < limit: break
        page += 1
    return sensors

def fetch_measurements_for_sensor(sensor_id: int, dt_from: str, dt_to: str,
                                  zone: str, ingest_date: str,
                                  limit=PAGE_LIMIT_DEFAULT, base_dir: str = "./raw_openaq") -> int:
    page, total = 1, 0
    while True:
        params = {"datetime_from": dt_from, "datetime_to": dt_to, "limit": limit, "page": page}
        r = get(f"{api_base()}/sensors/{sensor_id}/measurements", params=params)
        js = r.json()
        results = js.get("results", [])
        out_folder = build_out_folder(base_dir, zone, sensor_id, ingest_date)
        
        with open(os.path.join(out_folder, f"sensor-{sensor_id}_page-{page}.json"), "w", encoding="utf-8") as f:
            json.dump(js, f, ensure_ascii=False)
        total += len(results)
        if len(results) < limit: break
        page += 1
    return total

def fetch_measurements_for_sensor_raw(sensor_id: int, dt_from: str, dt_to: str, limit=PAGE_LIMIT_DEFAULT):
    """SOLO obtiene datos, NO los guarda"""
    pages = []
    page = 1
    while True:
        params = {"datetime_from": dt_from, "datetime_to": dt_to, "limit": limit, "page": page}
        r = get(f"{api_base()}/sensors/{sensor_id}/measurements", params=params)
        js = r.json()
        results = js.get("results", [])
        
        pages.append(js)  # Accumulate pages in memory
        
        if len(results) < limit: break
        page += 1
    return pages  # Return all pages