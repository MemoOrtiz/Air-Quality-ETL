# src/openaq_ingestion/cli/args_parser.py
import argparse
from pathlib import Path
from ..core.config import out_dir

def parse_arguments():
    """CLI arguments configuration for OpenAQ data extraction"""
    parser = argparse.ArgumentParser(
        description="OpenAQ Data Extraction - Air Quality Data Extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:
  # Full extraction for all zones
  python -m openaq_ingestion.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z

  # Only a specific zone
  python -m openaq_ingestion.main --zone Monterrey_ZMM --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z

  # Specify output directory
  python -m openaq_ingestion.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z --out ./my_data

  # Different zones configuration file
  python -m openaq_ingestion.main --zones ./custom_zones.json --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
        """
    )
    
    # Calculate zones config path relative to this file
    current_dir = Path(__file__).parent.parent.parent.parent  # Back to project root
    default_zones_path = current_dir / "src" / "scripts" / "zones_config.json"
    
    parser.add_argument(
        "--zones", 
        default=str(default_zones_path),
        help=f"Zone configuration file (default: {default_zones_path})"
    )
    
    parser.add_argument(
        "--zone", 
        help="Process only this specific zone by name (e.g., 'Monterrey_ZMM')"
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
    
    return parser.parse_args()
