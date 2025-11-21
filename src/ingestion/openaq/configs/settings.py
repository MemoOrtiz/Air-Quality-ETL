# src/ingestion/openaq/configs/settings.py
import os
from dotenv import load_dotenv

PAGE_LIMIT_DEFAULT = 1000

def load_env():
    load_dotenv()

def api_base():
    """
    Load API base URL from .env
    """
    api_base = os.getenv("API_BASE")
    if not api_base:
        raise ValueError(
            "API_BASE is not configured in .env\n"
            "Add: API_BASE=https://api.openaq.org/v3"
        )
    return api_base

def api_headers():
    """
    Generate headers for OpenAQ API requests
    """
    key = os.getenv("OPENAQ_API_KEY")
    if not key:
        raise ValueError(
            "OPENAQ_API_KEY is not configured in .env\n"
            "Get your API key at: https://openaq.org/#/register\n"
            "Add: OPENAQ_API_KEY=your_api_key_here"
        )
    return {"X-API-Key": key.strip()}

def out_dir():
    """
    Load output directory from .env
    Raises an exception if not configured to enforce explicit configuration
    """
    out_dir_value = os.getenv("OUT_DIR")
    if not out_dir_value:
        raise ValueError(
            "OUT_DIR is not configured in .env\n"
            "Add: OUT_DIR=./bronze (or your preferred directory path)"
        )
    return out_dir_value

def s3_bucket():
    """
    Load S3 bucket name from .env
    Returns None if not configured (allows using local storage)
    """
    return os.getenv("AWS_S3_BUCKET_NAME")

def s3_prefix():
    """
    Load S3 prefix/folder from .env
    Defaults to 'bronze' for medallion architecture
    """
    return os.getenv("AWS_S3_PREFIX", "bronze")

def storage_mode():
    """
    Determine storage mode based on S3 configuration
    Returns: 's3' if S3_BUCKET_NAME is configured, 'local' otherwise
    """
    return "s3" if s3_bucket() else "local"