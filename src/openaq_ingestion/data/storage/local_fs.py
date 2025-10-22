#local_fs = local file system
# src/openaq_ingestion/data/storage/local_fs.py
import os, json
from datetime import datetime
from ...utils.helpers import ensure_dir

class RawLocal:
    def __init__(self, base="./raw/openaq"):
        self.base = base

    def zone_dir(self, zone):
        """Base directory for a zone: raw/openaq/ZONE_NAME"""
        p = os.path.join(self.base, zone) 
        ensure_dir(p) 
        return p

    def metadata_dir(self, zone, ingest_date):
        """Metadata directory: raw/openaq/Monterrey/metadata/ingest_date=YYYY-MM-DD"""
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
 
    def save_measurements_pages(self, zone, sensor_id, pages_data, ingest_date):
        """ Save measurement pages for a sensor """
        #Build out folder comes from utils
        folder = self.measurements_pages_dir(zone, sensor_id, ingest_date) 
        for page_num, page_data in enumerate(pages_data, 1):
            file_path = os.path.join(folder, f"sensor-{sensor_id}_page-{page_num}.json")
            self.save_json(file_path, page_data)

    # New methods for date-based directories
    def measurements_pages_dir(self, zone, sensor_id, ingest_date):
        """Pages directory: raw/openaq/Monterrey/measurements/pages/ingest_date=YYYY-MM-DD/sensor_id=####"""
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
        """Event date directory: raw/openaq/Monterrey/measurements/event_date/year=YYYY/month=MM/day=DD/sensor_id=####"""
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
        """Save measurements organized by event date in JSONL format"""
        for event_date, measurements in measurements_by_date.items():
            event_dir = self.measurements_event_date_dir(zone, sensor_id, event_date)
            file_path = os.path.join(event_dir, f"sensor-{sensor_id}_{event_date}.jsonl")
            
            with open(file_path, "w", encoding="utf-8") as f:
                for measurement in measurements:
                    json.dump(measurement, f, ensure_ascii=False)
                    f.write('\n')