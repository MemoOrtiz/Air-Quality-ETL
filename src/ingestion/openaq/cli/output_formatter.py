# src/ingestion/openaq/cli/output_formatter.py

def print_header():
    """Print initial banner for the data extraction process"""
    print("-" * 40)
    print("    OpenAQ Data Extraction - Air Quality Data Extraction")
    print("-" * 40)

def print_final_summary(total_stats: dict, zones: list, output_dir: str, ingest_date: str):
    """
    Print comprehensive final summary of the extraction process
    
    Args:
        total_stats: Dictionary with aggregated statistics
        zones: List of processed zones
        output_dir: Base output directory
        ingest_date: Date of the data ingestion
    """
    print("\n" + "-" * 40)
    print("         EXTRACTION COMPLETED        ")
    print("-" * 40)
    
    # Statistics summary
    print("\nFinal Statistics:")
    print("-" * 40)
    print(f"Processed zones:      {total_stats['zones']:,}")
    print(f"Total locations:      {total_stats['locations']:,}")
    print(f"Total sensors:        {total_stats['sensors']:,}")
    print(f"Total measurements:   {total_stats['measurements']:,}")
    
    if total_stats['errors'] > 0:
        print(f"Errors encountered:   {total_stats['errors']:,}")
    
    print(f"Ingestion date:       {ingest_date}")
    
    # Directory structure overview
    print(f"\nData Structure saved in: {output_dir}/")
    print("-" * 40)
    
    for zone in zones:
        zone_name = zone["name"]
        print(f" {zone_name}/")
        print(f"   ├──  measurements/")
        print(f"   │   └──  ingest_date={ingest_date}/sensor_id=*/")
        print(f"   │       └──  page-*.json")
        print(f"   └──  metadata/ingest_date={ingest_date}/")
        print(f"       ├──  locations_index.json")
        print(f"       ├──  sensors_by_location/location_id=*.json")
        print(f"       └──  sensors_index.json")
        print()
    
    print("-" * 40)

def print_zone_summary(zone_name: str, zone_stats: dict):
    """Print summary for a completed zone"""
    print(f"\n Zone '{zone_name}' completed:")
    print(f"   - Locations: {zone_stats['locations']}")
    print(f"   - Sensors: {zone_stats['sensors']}")
    print(f"   - Measurements: {zone_stats['measurements']}")
    if zone_stats['errors'] > 0:
        print(f"   - Errors: {zone_stats['errors']}")

def print_process_info(api_base: str, page_limit: int, output_dir: str, ingest_date: str):
    """Print process configuration information"""
    print(f"API OpenAQ:           {api_base}")
    print(f"Page limit:           {page_limit}")
    print(f"Base directory:       {output_dir}")
    print(f"Ingestion date:       {ingest_date}")
    print()