# src/openaq_ingestion/etl/zone_processor.py
from datetime import datetime
from ..data.fetchers import (
    fetch_locations_bbox,
    fetch_sensors_for_location, 
    fetch_measurements_for_sensor_raw
)
from ..data.storage.storage_interface import StorageInterface

class ZoneProcessor:
    """Process individual zones for ETL operations"""
    
    def __init__(self, storage: StorageInterface):
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
        
        # Filter sensors based on activity period
        active_sensors = self._filter_active_sensors(all_sensors, dt_from, dt_to)
        skipped = len(all_sensors) - len(active_sensors)
        
        if skipped > 0:
            print(f"Skipping {skipped} inactive sensors (no overlap with requested period)")
        
        print(f"Processing {len(active_sensors)} active sensors...")
        
        total_measurements = 0
        
        for i, sensor_info in enumerate(active_sensors, 1):
            sensor_id = sensor_info["sensorId"]
            parameter = sensor_info["parameter"]
            location_name = sensor_info["locationName"]
            
            print(f"   [{i:2d}/{len(active_sensors)}] Sensor {sensor_id} ({parameter}) - {location_name}")
            
            try:
                # Obtain measurements using fetchers (raw version)
                pages_data = fetch_measurements_for_sensor_raw(
                    sensor_id=sensor_id,
                    dt_from=dt_from,
                    dt_to=dt_to
                )
                
                if pages_data:
                    # # Organize by event date and save in event_date/ directory
                    # measurements_by_date = self._organize_by_event_date(pages_data)
                    # self.storage.save_measurements_by_event_date(zone_name, sensor_id, measurements_by_date)
                    
                    # ========== OPTION 2: BRONZE ONLY (recommended for AWS/Cloud) ==========
                    # Raw extraction only, no processing (faster)
                    # Processing will be done in Silver layer separately
                    self.storage.save_measurements_raw(zone_name, sensor_id, pages_data, ingest_date)
                    
                    # Count total measurements
                    measurements_count = sum(len(page.get("results", [])) for page in pages_data)
                    total_measurements += measurements_count
                    
                    print(f"    -> {measurements_count} measurements in {len(pages_data)} pages")
                    print(f"       Raw data saved to Bronze layer ")
                else:
                    print(f"    -> No data in the specified range")
                    
            except Exception as e:
                print(f"      Error: {e}")
                continue
        
        return total_measurements
    
    def _organize_by_event_date(self, pages_data: list) -> dict:
        """
        Organize measurements by event date (the date when the measurement was taken)
        
        Args:
            pages_data: List of page responses from API
            
        Returns:
            Dict with event_date as key and list of measurements as value
            Example: {'2025-10-01': [measurement1, measurement2], '2025-10-02': [...]}
        """
        measurements_by_date = {}
        
        for page in pages_data:
            measurements = page.get("results", [])
            
            for measurement in measurements:
                # Extract event date from measurement
                # API returns: {"period": {"datetimeFrom": {"utc": "2025-09-17T00:00:00Z", "local": "..."}}}
                period_info = measurement.get("period", {})
                datetime_from = period_info.get("datetimeFrom", {})
                event_datetime = datetime_from.get("utc", "")
                
                if event_datetime:
                    # Extract just the date part: 2025-09-17
                    event_date = event_datetime[:10]  # "2025-09-17T00:00:00Z" -> "2025-09-17"
                    
                    if event_date not in measurements_by_date:
                        measurements_by_date[event_date] = []
                    
                    measurements_by_date[event_date].append(measurement)
                else:
                    # Handle measurements without proper date
                    if "unknown_date" not in measurements_by_date:
                        measurements_by_date["unknown_date"] = []
                    measurements_by_date["unknown_date"].append(measurement)
        
        return measurements_by_date
    
    def _filter_active_sensors(self, all_sensors: list, dt_from: str, dt_to: str) -> list:
        """
        Filter sensors that have data overlapping with the requested time period.
        This prevents unnecessary API calls to inactive sensors.
        
        Args:
            all_sensors: List of sensor info dicts with datetimeFirst and datetimeLast
            dt_from: Start of requested period (e.g., "2025-09-01")
            dt_to: End of requested period (e.g., "2025-10-31")
            
        Returns:
            List of sensors that potentially have data in the requested period
        """
        
        # Parse requested period
        try:
            request_start = datetime.fromisoformat(dt_from.replace('Z', '+00:00'))
            request_end = datetime.fromisoformat(dt_to.replace('Z', '+00:00'))
        except Exception as e:
            print(f" Warning: Could not parse date range, skipping filter: {e}")
            return all_sensors
        
        active_sensors = []
        
        for sensor in all_sensors:
            # Get sensor's activity period
            datetime_first = sensor.get('datetimeFirst', {})
            datetime_last = sensor.get('datetimeLast', {})
            
            # Handle missing metadata
            if not datetime_first or not datetime_last:
                # Include sensor if we don't have metadata (be conservative)
                active_sensors.append(sensor)
                continue
            
            sensor_first_str = datetime_first.get('utc', '')
            sensor_last_str = datetime_last.get('utc', '')
            
            if not sensor_first_str or not sensor_last_str:
                # Include sensor if dates are missing
                active_sensors.append(sensor)
                continue
            
            try:
                # Parse sensor's activity period
                sensor_start = datetime.fromisoformat(sensor_first_str.replace('Z', '+00:00'))
                sensor_end = datetime.fromisoformat(sensor_last_str.replace('Z', '+00:00'))
                
                # Check for overlap:
                # Sensor is active if: sensor_start <= request_end AND sensor_end >= request_start
                has_overlap = sensor_start <= request_end and sensor_end >= request_start
                
                if has_overlap:
                    active_sensors.append(sensor)
                    
            except Exception as e:
                # If parsing fails, include sensor (be conservative)
                print(f"Warning: Could not parse dates for sensor {sensor.get('sensorId')}: {e}")
                active_sensors.append(sensor)
                continue
        
        return active_sensors