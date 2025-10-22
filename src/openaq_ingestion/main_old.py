"""
OpenAQ ETL Main Script
Simplified entry point using modular architecture

Usage:
    python -m src.openaq_ingestion.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
    python -m src.openaq_ingestion.main --zone Monterrey_ZMM --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
"""

from .core.config import load_env
from .cli.args_parser import parse_arguments
from .etl.orchestrator import DataIngestionOrchestrator

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

def run_zone_etl(zone_name: str, bbox: tuple, dt_from: str, dt_to: str, storage: RawLocal, ingest_date: str):
    """
    Run the ETL process for a specific zone:
    1. Fetch locations (fetchers)
    2. Fetch sensors (fetchers)  
    3. Fetch measurements (fetchers)
    4. Save everything locally (storage)
    """

    print(f"\nProcessing zone: {zone_name}")
    print(f"Geographic area: {bbox}")
    print(f"Time range: {dt_from} → {dt_to}")
    print("-" * 60)
    
    zone_stats = {
        'locations': 0,
        'sensors': 0, 
        'measurements': 0,
        'errors': 0
    }
    try:
        # ========= PASO 1: LOCATIONS =========
        print("[1/3] Loading locations...")
        locations = fetch_locations_bbox(bbox)
        zone_stats['locations'] = len(locations)
        
        if not locations:
            print("    No locations found in this area. Skipping zone.")
            return zone_stats

        print(f"   {len(locations)} locations found")

        # Save locations using storage
        storage.save_locations_index(zone_name, locations, ingest_date)
        print(f"   Saved: locations_index.json")
    
    except Exception as e:
        print(f"\nFatal error processing zone {zone_name}: {e}")
        zone_stats['errors'] += 1

    # ========= PASO 2: SENSORS =========
        print("\n [2/3] Loading sensors for each location...")
        all_sensors = []
        
        for i, location in enumerate(locations, 1):
            loc_id = location.get("id")
            loc_name = location.get("name", "Unknown")
            city = location.get("city", "Unknown")
            
            print(f"   [{i:2d}/{len(locations)}] {loc_name} (ID: {loc_id})")
            
            try:
                # Obtain sensors using fetchers
                sensors = fetch_sensors_for_location(loc_id)
                print(f"      → {len(sensors)} sensors found")

                # Save sensors for this location using storage
                storage.save_sensors_for_location(zone_name, loc_id, sensors, ingest_date)

                # Prepare consolidated index
                for sensor in sensors:
                    sensor_info = {
                        "locationId": loc_id,
                        "locationName": loc_name,
                        "city": city,
                        "provider": location.get("provider", "Unknown"),
                        "sensorId": sensor.get("id"),
                        "parameter": (sensor.get("parameter") or {}).get("name", "Unknown"),
                        "units": (sensor.get("parameter") or {}).get("units", "Unknown"),
                        "datetimeFirst": sensor.get("datetimeFirst"),
                        "datetimeLast": sensor.get("datetimeLast"),
                    }
                    all_sensors.append(sensor_info)
                    
            except Exception as e:
                print(f"       Error loading sensors: {e}")
                zone_stats['errors'] += 1
                continue
        
        zone_stats['sensors'] = len(all_sensors)
        
        # Save consolidated sensors index using storage
        storage.save_sensors_index(zone_name, all_sensors, ingest_date)
        print(f"   Saved: sensors_index.json ({len(all_sensors)} total sensors)")
         # ========= STEP 3: MEASUREMENTS =========
        print(f"\n[3/3] Loading measurements from {len(all_sensors)} sensors...")

        for i, sensor_info in enumerate(all_sensors, 1):
            sensor_id = sensor_info["sensorId"]
            parameter = sensor_info["parameter"]
            location_name = sensor_info["locationName"]
            
            print(f"   [{i:2d}/{len(all_sensors)}] Sensor {sensor_id} ({parameter}) - {location_name}")
            
            try:
                # Obtain measurements using fetchers (raw version)
                pages_data = fetch_measurements_for_sensor_raw(
                    sensor_id=sensor_id,
                    dt_from=dt_from,
                    dt_to=dt_to
                )
                
                if pages_data:
                    # SAVE THE DATA USING "storage/local_fs.py"
                    storage.save_measurements_pages(zone_name, sensor_id, pages_data, ingest_date)

                    # Count total measurements
                    measurements_count = sum(len(page.get("results", [])) for page in pages_data)
                    zone_stats['measurements'] += measurements_count

                    print(f"    -> {measurements_count} measurements in {len(pages_data)} pages")
                else:
                    print(f"    -> No data in the specified range")

            except Exception as e:
                print(f"      Error: {e}")
                zone_stats['errors'] += 1
                continue

        print(f"\nZone {zone_name} completed successfully")
    except Exception as e:
        print(f"\nFatal error processing zone {zone_name}: {e}")
        zone_stats['errors'] += 1
    
    return zone_stats

def parse_arguments():
    """CLI arguments configuration"""
    parser = argparse.ArgumentParser(
        description="OpenAQ ETL - Air Quality Data Extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:
  # Full ETL for all zones
  python -m src.openaq_ingestion.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z

  # Only a specific zone
  python -m src.openaq_ingestion.main --zone Monterrey_ZMM --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z

  # Specify output directory
  python -m src.openaq_ingestion.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z --out ./mi_data
        """
    )
    
    parser.add_argument(
        "--zones", 
        default="./src/scripts/zones_config.json",
        help="File of zone configuration (default: ../scripts/zones_config.json)"
    )
    
    parser.add_argument(
        "--zone", 
        help="Process only this specific zone (by name)"
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

def main():
    """Main ETL function"""
    
    print_header()

    # Load configuration
    load_env()
    args = parse_arguments()
    
    print(f"API OpenAQ: {api_base()}")
    print(f"Page limit: {PAGE_LIMIT_DEFAULT}")
    print(f"Base directory: {args.out_base}")
    print(f"Ingest date: {ingest_date_utc()}")
    print()

    # Load zones
    zones = load_zones_config(args.zones)
    # Filter specific zone if specified
    if args.zone:
        zones = [z for z in zones if z["name"] == args.zone]
        if not zones:
            print(f"Error: Zone '{args.zone}' not found in {args.zones}")
            available_zones = [z["name"] for z in load_zones_config(args.zones)]
            print(f"Available zones: {', '.join(available_zones)}")
            sys.exit(1)

    print(f"Zones to process: {len(zones)}")
    for zone in zones:
        print(f"    • {zone['name']}: {zone['bbox']}")
    print()

    # Initialize storage
    storage = RawLocal(base=args.out_base)
    ingest_date = ingest_date_utc()

    # Global statistics
    total_stats = {
        'zones': len(zones),
        'locations': 0,
        'sensors': 0,
        'measurements': 0,
        'errors': 0
    }

    # Process each zone
    try:
        for zone in zones:
            zone_stats = run_zone_etl(
                zone_name=zone["name"],
                bbox=tuple(zone["bbox"]),
                dt_from=args.dt_from,
                dt_to=args.dt_to,
                storage=storage,
                ingest_date=ingest_date
            )

            # Accumulate statistics
            for key in ['locations', 'sensors', 'measurements', 'errors']:
                total_stats[key] += zone_stats[key]

        # ------- FINAL SUMMARY --------
        print("\n Complete")
        print("-" * 40)
        print("\nFinal statistics:")
        print(f"Processed zones: {total_stats['zones']}")
        print(f"Total locations: {total_stats['locations']}")
        print(f"Total sensors: {total_stats['sensors']}")
        print(f"Total measurements: {total_stats['measurements']}")
        if total_stats['errors'] > 0:
            print(f"Errors: {total_stats['errors']}")
        print(f"Ingest date: {ingest_date}")
        
        print(f"\nGeneral Structure save in: {args.out_base}/")
        for zone in zones:
            zone_name = zone["name"]
            print(f"   {zone_name}/")
            print(f"   ├── metadata/ingest_date={ingest_date}/")
            print(f"   │   ├── locations_index.json")
            print(f"   │   ├── sensors_loc-*.json") 
            print(f"   │   └── sensors_index.json")
            print(f"   └── measurements/ingest_date={ingest_date}/sensor_id=*/")
            print(f"       └── sensor-*_page-*.json")
        
        print("-" * 40)
        
        if total_stats['errors'] > 0:
            print(f" Proceso completado con {total_stats['errors']} errores")
            sys.exit(1)
        else:
            print("Process completed successfully")
            
    except KeyboardInterrupt:
        print("\nETL interrumped by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error:: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()