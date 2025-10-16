#local_fs = local file system
# src/openaq_ingestion/storage/local_fs.py
import os, json
from ..utils import build_out_folder, ensure_dir

class RawLocal:
    def __init__(self, base="./raw_openaq"):
        self.base = base

    def zone_dir(self, zone):
        p = os.path.join(self.base, zone); ensure_dir(p); return p

    def metadata_dir(self, zone, ingest_date):
        p = os.path.join(self.zone_dir(zone), "metadata", f"ingest_date={ingest_date}")
        ensure_dir(p); return p

    def save_json(self, path: str, data: dict):
        ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def save_locations_index(self, zone, locations, ingest_date):
        p = os.path.join(self.metadata_dir(zone, ingest_date), "locations_index.json")
        self.save_json(p, {"results": locations})

    def save_sensors_for_location(self, zone, loc_id, sensors, ingest_date):
        p = os.path.join(self.metadata_dir(zone, ingest_date), f"sensors_loc-{loc_id}.json")
        self.save_json(p, {"results": sensors})

    def save_sensors_index(self, zone, sensors_idx, ingest_date):
        p = os.path.join(self.metadata_dir(zone, ingest_date), "sensors_index.json")
        self.save_json(p, sensors_idx)
    
    def save_measurements_page(self, zone, sensor_id, page_data, page_num, ingest_date):
        folder = build_out_folder(self.base, zone, sensor_id, ingest_date)
        file_path = os.path.join(folder, f"sensor-{sensor_id}_page-{page_num}.json")
        self.save_json(file_path, page_data)
        
    def save_measurements_pages(self, zone, sensor_id, pages_data, ingest_date):
        for page_num, page_data in enumerate(pages_data, 1):
            folder = build_out_folder(self.base, zone, sensor_id, ingest_date)
            file_path = os.path.join(folder, f"sensor-{sensor_id}_page-{page_num}.json")
            self.save_json(file_path, page_data)