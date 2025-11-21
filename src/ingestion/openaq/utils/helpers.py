# src/ingestion/openaq/utils/helpers.py
import os, re
from pathlib import Path
from datetime import datetime, timezone

def ensure_dir(path: str):
    """Create directory structure if it doesn't exist"""
    Path(path).mkdir(parents=True, exist_ok=True)

def ingest_date_utc() -> str:
    """Get current UTC date in YYYY-MM-DD format for data ingestion tracking"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def slugify(s: str) -> str:
    """Convert string to URL-safe slug format"""
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-_.]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-") or "unknown"