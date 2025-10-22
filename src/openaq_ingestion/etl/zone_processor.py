# src/openaq_ingestion/etl/zone_processor.py
from ..data.fetchers import (
    fetch_locations_bbox,
    fetch_sensors_for_location, 
    fetch_measurements_for_sensor_raw
)
from ..data.storage.local_fs import RawLocal

class ZoneProcessor:
    """Process individual zones for ETL operations"""
    
    def __init__(self, storage: RawLocal):
        self.storage = storage
    
    def extract_zone_data(self, zone_name: str, bbox: tuple, dt_from: str, dt_to: str, ingest_date: str) -> dict:
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
            # Process locations
            locations = self._process_locations(zone_name, bbox, ingest_date)
            zone_stats['locations'] = len(locations)
            
            if not locations:
                print("    No locations found in this area. Skipping zone.")
                return zone_stats
            
            # Process sensors
            all_sensors = self._process_sensors(zone_name, locations, ingest_date)
            zone_stats['sensors'] = len(all_sensors)
            
            # Process measurements
            measurements_count = self._process_measurements(zone_name, all_sensors, dt_from, dt_to, ingest_date)
            zone_stats['measurements'] = measurements_count
            
            print(f"\nZone {zone_name} completed successfully")
            
        except Exception as e:
            print(f"\nFatal error processing zone {zone_name}: {e}")
            zone_stats['errors'] += 1
        
        return zone_stats
    
    def _process_locations(self, zone_name: str, bbox: tuple, ingest_date: str) -> list:
        """Process locations for a zone"""
        print("[1/3] Loading locations...")
        locations = fetch_locations_bbox(bbox)
        print(f"   {len(locations)} locations found")
        
        # Save locations using storage
        self.storage.save_locations_index(zone_name, locations, ingest_date)
        print(f"   Saved: locations_index.json")
        
        return locations
    
    def _process_sensors(self, zone_name: str, locations: list, ingest_date: str) -> list:
        """Process sensors for all locations in a zone"""
        print("\n[2/3] Loading sensors for each location...")
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
                self.storage.save_sensors_for_location(zone_name, loc_id, sensors, ingest_date)
                
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
                continue
        
        # Save consolidated sensors index using storage
        self.storage.save_sensors_index(zone_name, all_sensors, ingest_date)
        print(f"   Saved: sensors_index.json ({len(all_sensors)} total sensors)")
        
        return all_sensors
    
    def _process_measurements(self, zone_name: str, all_sensors: list, dt_from: str, dt_to: str, ingest_date: str) -> int:
        """Process measurements for all sensors in a zone"""
        print(f"\n[3/3] Loading measurements from {len(all_sensors)} sensors...")
        
        total_measurements = 0
        
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
                    # Save using storage
                    self.storage.save_measurements_pages(zone_name, sensor_id, pages_data, ingest_date)
                    
                    # Count total measurements
                    measurements_count = sum(len(page.get("results", [])) for page in pages_data)
                    total_measurements += measurements_count
                    
                    print(f"    -> {measurements_count} measurements in {len(pages_data)} pages")
                else:
                    print(f"    -> No data in the specified range")
                    
            except Exception as e:
                print(f"      Error: {e}")
                continue
        
        return total_measurements