"""
OpenAQ ETL Main Script
Integrate fetchers + local storage using the entire project architecture

Usage:
    python -m src.openaq_ingestion.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
    python -m src.openaq_ingestion.main --zone Monterrey_ZMM --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
"""

import argparse
import json
import os
import sys
from datetime import datetime

# Imports related to the project
from .config import load_env, out_dir, API_BASE, PAGE_LIMIT_DEFAULT
from .utils import ingest_date_utc, slugify
from .fetchers import (
    fetch_locations_bbox,
    fetch_sensors_for_location, 
    fetch_measurements_for_sensor_raw  
)
from .storage.local_fs import RawLocal

def load_zones_config(path: str):
    """Load zone configuration from JSON"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("zones", [])
    except FileNotFoundError:
        print(f"Error: File {path} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error JSON in {path}: {e}")
        sys.exit(1)

def print_header():
    """Initial Banner ETL"""
    print("-" * 58)
    print("    OpenAQ ETL - Air Quality Data Extraction")
    print("-" * 58)

def run_zone_etl(zone_name: str, bbox: tuple, dt_from: str, dt_to: str, storage: RawLocal, ingest_date: str):
    """
    Run the ETL process for a specific zone:
    1. Fetch locations (fetchers)
    2. Fetch sensors (fetchers)  
    3. Fetch measurements (fetchers)
    4. Save everything locally (storage)
    """

    print(f"\nProcessing zone: {zone_name}")
    print(f"Geographic area: {bbox}")
    print(f"Time range: {dt_from} → {dt_to}")
    print("-" * 60)
    
    zone_stats = {
        'locations': 0,
        'sensors': 0, 
        'measurements': 0,
        'errors': 0
    }
    try:
        # ========= PASO 1: LOCATIONS =========
        print("[1/3] Loading locations...")
        locations = fetch_locations_bbox(bbox)
        zone_stats['locations'] = len(locations)
        
        if not locations:
            print("    No locations found in this area. Skipping zone.")
            return zone_stats

        print(f"   {len(locations)} locations found")

        # Save locations using storage
        storage.save_locations_index(zone_name, locations, ingest_date)
        print(f"   Saved: locations_index.json")
    
    except Exception as e:
        print(f"\nFatal error processing zone {zone_name}: {e}")
        zone_stats['errors'] += 1
    # ========= PASO 2: SENSORS =========
        print("\n [2/3] Loading sensors for each location...")
        all_sensors = []
        
        for i, location in enumerate(locations, 1):
            loc_id = location.get("id")
            loc_name = location.get("name", "Unknown")
            city = location.get("city", "Unknown")
            
            print(f"   [{i:2d}/{len(locations)}] {loc_name} (ID: {loc_id})")
            
            try:
                # Obtain sensors using fetchers
                sensors = fetch_sensors_for_location(loc_id)
                print(f"      → {len(sensors)} sensors found")

                # Save sensors for this location using storage
                storage.save_sensors_for_location(zone_name, loc_id, sensors, ingest_date)

                # Prepare consolidated index
                for sensor in sensors:
                    sensor_info = {
                        "locationId": loc_id,
                        "locationName": loc_name,
                        "city": city,
                        "provider": location.get("provider", "Unknown"),
                        "sensorId": sensor.get("id"),
                        "parameter": (sensor.get("parameter") or {}).get("name", "Unknown"),
                        "units": (sensor.get("parameter") or {}).get("units", "Unknown"),
                        "datetimeFirst": sensor.get("datetimeFirst"),
                        "datetimeLast": sensor.get("datetimeLast"),
                    }
                    all_sensors.append(sensor_info)
                    
            except Exception as e:
                print(f"       Error loading sensors: {e}")
                zone_stats['errors'] += 1
                continue
        
        zone_stats['sensors'] = len(all_sensors)
        
        # Save consolidated sensors index using storage
        storage.save_sensors_index(zone_name, all_sensors, ingest_date)
        print(f"   Saved: sensors_index.json ({len(all_sensors)} total sensors)")

    return zone_stats
