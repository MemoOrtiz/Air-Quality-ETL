import os
import time
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

def sleep_by_rate(resp):
    """Rate-limit of OpenAQ (60/min, 2000/hour)"""
    remaining = int(resp.headers.get("x-ratelimit-remaining", "60") or 60)
    reset = int(resp.headers.get("x-ratelimit-reset", "1") or 1)
    
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