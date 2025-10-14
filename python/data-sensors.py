import os
import time
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests

# -------------------------
# CONFIG
# -------------------------

BBOX = (-100.60, 25.50, -99.95, 25.85)  # longitudeW, latitudeS, longitudeE, latitudeN
DATETIME_FROM = "2025-09-01T00:00:00Z"
DATETIME_TO   = "2025-10-13T23:59:59Z"
PAGE_LIMIT = 1000
OUT_DIR = "./raw_openaq"

API_BASE = "https://api.openaq.org/v3"

#Load the .env file
load_dotenv()

# Load API Key 
api_key = os.getenv('OPENAQ_API_KEY')
if api_key:
    print("API Key loaded successfully.")

# Use the API Key in a request header
headers = {
    'X-API-Key': api_key  
}

def sleep_by_rate(api_response):
    """Rate-limit of OpenAQ (60/min, 2000/hour)"""
    remaining = int(api_response.headers.get("x-ratelimit-remaining", "60") or 60)
    reset = int(api_response.headers.get("x-ratelimit-reset", "1") or 1)
    
    if remaining <= 0:
        print(f"Rate limit reached. waiting {reset}s ...")
        time.sleep(max(reset, 1))
    elif remaining <= 5:
        print("Few requests remaining, slowing down...")
        time.sleep(2)
    else:
        time.sleep(1.2)  # ~50 req/min (seguro bajo 60)

def get(url, params=None, max_retries=5):
    for i in range(max_retries):
        request = requests.get(url, params=params or {}, headers=headers, timeout=60)
        if request.status_code == 200:
            sleep_by_rate(request)
            return request
        if request.status_code == 429:
            reset = int(request.headers.get("x-ratelimit-reset", "2") or 2)
            time.sleep(max(reset, 2))
            continue
        # another errors → retry 
        time.sleep(2)
    request.raise_for_status()
    return request

def fetch_locations_bbox(bbox, limit=1000):
    lonW, latS, lonE, latN = bbox
    page = 1
    results = []
    while True:
        params = {
            "bbox": f"{lonW},{latS},{lonE},{latN}",
            "limit": limit,
            "page": page
        }

        request_location = get(f"{API_BASE}/locations", params=params)
        chunk = request_location.json().get("results", [])
        results.extend(chunk)
        if len(chunk) < limit:
            break
        page += 1
    return results


def fetch_sensors_for_location(location_id, limit=1000):
    page = 1
    sensors = []
    while True:
        request_sensors = get(f"{API_BASE}/locations/{location_id}/sensors",
                params={"limit": limit, "page": page})
        chunk = request_sensors.json().get("results", [])
        sensors.extend(chunk)
        if len(chunk) < limit:
            break
        page += 1
    return sensors

def fetch_measurements_for_sensor(sensor_id, datetime_from, datetime_to, limit=1000, out_folder="."):
    page = 1
    total = 0
    while True:
        params = {
            "datetime_from": datetime_from,
            "datetime_to": datetime_to,
            "limit": limit,
            "page": page
        }
        request_measurements = get(f"{API_BASE}/sensors/{sensor_id}/measurements", params=params)
        js = request_measurements.json()
        results = js.get("results", [])
        # Guardar crudo por página
        with open(os.path.join(out_folder, f"sensor-{sensor_id}_page-{page}.json"),
                  "w", encoding="utf-8") as f:
            json.dump(js, f, ensure_ascii=False)
        total += len(results)
        if len(results) < limit:
            break
        page += 1
    return total
