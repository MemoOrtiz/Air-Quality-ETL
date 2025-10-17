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


