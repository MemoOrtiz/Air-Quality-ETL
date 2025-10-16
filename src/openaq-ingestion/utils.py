# src/openaq_ingestion/utils.py
import os, re
from pathlib import Path
from datetime import datetime, timezone

def ensure_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def ingest_date_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-_.]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-") or "unknown"

def build_out_folder(base_dir: str, zone: str, sensor_id: int, ingest_date: str) -> str:
    p = os.path.join(base_dir, zone, "measurements", f"ingest_date={ingest_date}", f"sensor_id={sensor_id}")
    ensure_dir(p)
    return p