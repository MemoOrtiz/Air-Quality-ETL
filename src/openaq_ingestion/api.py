import time
import requests
from .config import api_headers

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
        request = requests.get(url, params=params or {}, headers=api_headers(), timeout=60)
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