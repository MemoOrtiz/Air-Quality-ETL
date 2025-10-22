# src/openaq_ingestion/config.py
import os
from dotenv import load_dotenv

API_BASE = "https://api.openaq.org/v3"
PAGE_LIMIT_DEFAULT = 1000
OUT_DIR_DEFAULT = "./raw_openaq"

def load_env():
    load_dotenv()

def api_headers():
    key = os.getenv("OPENAQ_API_KEY") or ""
    return {"X-API-Key": key} if key else {}

def out_dir():
    return os.getenv("OUT_DIR", OUT_DIR_DEFAULT)