#local_fs = local file system
# src/openaq_ingestion/data/storage/local_fs.py
import os, json
from datetime import datetime
from ...utils.helpers import ensure_dir
from .storage_interface import StorageInterface

class LocalStorage(StorageInterface):
    def __init__(self, base="./raw"):
        self.base = base

    def zone_dir(self, zone):
        """Base directory for a zone: raw/zone={zone_name}"""
        p = os.path.join(self.base, f"zone={zone}") 
        ensure_dir(p) 
        return p

    def metadata_dir(self, zone, ingest_date):
        """Metadata directory: raw/zone={zone_name}/metadata/ingest_date={YYYY-MM-DD}"""
        p = os.path.join(self.zone_dir(zone), "metadata", f"ingest_date={ingest_date}")
        ensure_dir(p); return p

    def save_json(self, path: str, data: dict):
        """ Save a dictionary as a JSON file """
        ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def save_locations_index(self, zone, locations, ingest_date):
        """ Save locations index if not exists """
        p = os.path.join(self.metadata_dir(zone, ingest_date), "locations_index.json")
        # Only if it doesn't exist
        if not os.path.exists(p):
            self.save_json(p, {"results": locations})
            return True  # created
        return False  # already exists

    def save_sensors_for_location(self, zone, loc_id, sensors, ingest_date):
        """ Save sensors for a location if not exists """
        p = os.path.join(self.metadata_dir(zone, ingest_date), f"sensors_loc-{loc_id}.json")
        if not os.path.exists(p):
            self.save_json(p, {"results": sensors})
            return True  # created
        return False  # already exists

    def save_sensors_index(self, zone, sensors_idx, ingest_date):
        """ Save sensors index if not exists """
        p = os.path.join(self.metadata_dir(zone, ingest_date), "sensors_index.json")
        if not os.path.exists(p):
            self.save_json(p, sensors_idx)
            return True  # created
        return False  # already exists
    
    # New methods for date-based directories
    def measurements_pages_dir(self, zone, sensor_id, ingest_date):
        """Pages directory: raw/zone={zone_name}/measurements/pages/ingest_date={YYYY-MM-DD}/sensor_id={id}"""
        p = os.path.join(
            self.zone_dir(zone), 
            "measurements", 
            "pages", 
            f"ingest_date={ingest_date}", 
            f"sensor_id={sensor_id}"
        )
        ensure_dir(p)
        return p
    
    def measurements_event_date_dir(self, zone, sensor_id, event_date):
        """Event date directory: raw/zone={zone_name}/measurements/event_date/year={YYYY}/month={MM}/day={DD}/sensor_id={id}"""
        dt = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
        p = os.path.join(
            self.zone_dir(zone), 
            "measurements", 
            "event_date",
            f"year={dt.year}",
            f"month={dt.month:02d}",
            f"day={dt.day:02d}",
            f"sensor_id={sensor_id}"
        )
        ensure_dir(p)
        return p
    
    def save_measurements_by_event_date(self, zone, sensor_id, measurements_by_date):
        """
        Save measurements organized by event date in JSONL format
        (Silver layer - used in local ETL with immediate transformation)
        For Medallion architecture, this should be executed in a separate step after Bronze
        """
        for event_date, measurements in measurements_by_date.items():
            # Handle special case for unknown dates
            if event_date == "unknown_date":
                # Create a special directory for unknown dates
                event_dir = os.path.join(
                    self.zone_dir(zone), 
                    "measurements", 
                    "event_date",
                    "unknown",
                    f"sensor_id={sensor_id}"
                )
                ensure_dir(event_dir)
            else:
                # Use normal event date directory structure
                event_dir = self.measurements_event_date_dir(zone, sensor_id, event_date)
            
            file_path = os.path.join(event_dir, f"sensor-{sensor_id}_{event_date}.jsonl")
            
            with open(file_path, "w", encoding="utf-8") as f:
                for measurement in measurements:
                    json.dump(measurement, f, ensure_ascii=False)
                    f.write('\n')
    
    # ------------ BRONZE LAYER METHOD  -----------
    
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        """
        Save raw measurement pages without processing (BRONZE LAYER)
        
        This method is ideal for Medallion architecture (Bronze → Silver → Gold):
        - Only saves raw API responses without processing
        - Faster during extraction (no data processing)
        - Allows re-processing later without re-extracting from API
        - Recommended for AWS: S3 Bronze → Lambda/Glue transforms to Silver
        
        Resulting structure:
        raw/zone={zone}/measurements/pages/ingest_date={YYYY-MM-DD}/sensor_id={id}/
        ├── page-1.json  (complete API response)
        ├── page-2.json
        └── page-N.json
        
        Args:
            zone: Zone name (e.g., 'Monterrey_ZMM')
            sensor_id: Sensor ID
            pages_data: List of complete API responses (unprocessed)
            ingest_date: Ingestion date in YYYY-MM-DD format
        """
        folder = self.measurements_pages_dir(zone, sensor_id, ingest_date)
        
        for page_num, page_data in enumerate(pages_data, 1):
            file_path = os.path.join(folder, f"page-{page_num}.json")
            self.save_json(file_path, page_data)