# src/ingestion/openaq/utils/config_loader.py
import json
import sys

def load_zones_config(path: str) -> list:
    """
    Load zone configuration from JSON file
    
    Args:
        path: Path to zones configuration JSON file
        
    Returns:
        List of zone dictionaries with name, bbox, etc.
        
    Raises:
        SystemExit: If file not found or JSON parsing error
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            zones_data = json.load(f)
            zones = zones_data.get("zones", [])
            
            if not zones:
                print(f"Warning: No zones found in {path}")
                
            return zones
            
    except FileNotFoundError:
        print(f"Error: File {path} not found")
        print("Make sure the zones configuration file exists.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON in {path}: {e}")
        print("Check that the file contains valid JSON.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error loading zones config: {e}")
        sys.exit(1)