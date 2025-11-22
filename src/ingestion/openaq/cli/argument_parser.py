# src/ingestion/openaq/cli/argument_parser.py
import argparse
from pathlib import Path
from ..configs.settings import out_dir

def parse_arguments():
    """CLI arguments configuration for OpenAQ data extraction"""
    parser = argparse.ArgumentParser(
        description="OpenAQ Data Extraction - Air Quality Data Extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:

  --BASIC USAGE:
  
  # Extract 1 day of data for all zones (auto-detect storage)
  python -m src.main --from 2025-11-14T00:00:00Z --to 2025-11-14T23:59:59Z
  
  # Extract data for a specific zone
  python -m src.main --zone Monterrey_Metropolitan --from 2025-11-01T00:00:00Z --to 2025-11-15T23:59:59Z
  
  --STORAGE OPTIONS:
  
  # Force LOCAL storage (saves to directory)
  python -m src.main --storage local --zone Guadalajara_Metropolitan --from 2025-11-14T00:00:00Z --to 2025-11-14T23:59:59Z
  
  # Force S3 storage (requires AWS_S3_BUCKET_NAME in .env)
  python -m src.main --storage s3 --zone Monterrey_Metropolitan --from 2025-11-14T00:00:00Z --to 2025-11-14T23:59:59Z
  
  --ADVANCED OPTIONS:
  
  # Custom output directory (local storage only)
  python -m src.main --storage local --out ./my_data --from 2025-11-01T00:00:00Z --to 2025-11-15T23:59:59Z
  
  # Custom zones config file
  python -m src.main --zones ./custom_zones.json --from 2025-11-01T00:00:00Z --to 2025-11-15T23:59:59Z
  
  # Full month to S3
  python -m src.main --storage s3 --from 2025-10-01T00:00:00Z --to 2025-10-31T23:59:59Z
  
  --QUICK TESTS:
  
  # Test with 1 hour of data (fastest)
  python -m src.main --zone Monterrey_Metropolitan --from 2025-11-14T00:00:00Z --to 2025-11-14T01:00:00Z
  
  # Test S3 with minimal data
  python -m src.main --storage s3 --zone CDMX_Metropolitan --from 2025-11-14T12:00:00Z --to 2025-11-14T13:00:00Z

Available zones: Monterrey_Metropolitan, CDMX_Metropolitan, Guadalajara_Metropolitan
        """
    )
    
    # Calculate zones config path relative to this file
    current_dir = Path(__file__).parent.parent  # Back to openaq directory
    default_zones_path = current_dir / "configs" / "zones_config.json"
    
    parser.add_argument(
        "--zones", 
        default=str(default_zones_path),
        help=f"Zone configuration file (default: {default_zones_path})"
    )
    
    parser.add_argument(
        "--zone", 
        help="Process only this specific zone by name (e.g., 'Monterrey_Metropolitan')"
    )
    
    parser.add_argument(
        "--from", 
        dest="dt_from", 
        required=True,
        help="Start date/time in ISO format (e.g., 2025-09-01T00:00:00Z)"
    )
    
    parser.add_argument(
        "--to", 
        dest="dt_to", 
        required=True,
        help="End date/time in ISO format (e.g., 2025-10-15T23:59:59Z)"
    )
    
    parser.add_argument(
        "--out", 
        dest="out_base", 
        default=out_dir(),
        help=f"Base output directory (default: {out_dir()})"
    )

    parser.add_argument(
        "--storage",
        choices=["local", "s3"],
        default=None,
        help="Storage backend: 'local' for filesystem, 's3' for AWS S3. If not specified, auto-detects based on S3_BUCKET_NAME in .env"
    )
    
    return parser.parse_args()
