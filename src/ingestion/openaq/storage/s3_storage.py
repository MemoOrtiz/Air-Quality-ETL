import json
import boto3
from dotenv import load_dotenv
from .storage_interface import StorageInterface

load_dotenv()
class S3Storage(StorageInterface):
    def __init__(self, bucket_name, prefix="bronze"):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.prefix = prefix
    
    def save_json(self, path: str, data: dict):

        json_string = json.dumps(data, ensure_ascii=False)
        json_bytes = json_string.encode('utf-8')

        s3_key = f"{self.prefix}/{path}" # example: "bronze/zone=X/file.json"

        self.s3.put_object(
            Bucket = self.bucket,
            Key = s3_key,
            Body = json_bytes,
            ContentType = 'application/json'  # Specify that it's JSON
        )

    def zone_dir(self, zone):
        return f"zone={zone}" 

    def metadata_dir(self, zone, ingest_date):
        return f"{self.zone_dir(zone)}/metadata/ingest_date={ingest_date}"
    # Returns: "zone=Monterrey/metadata/ingest_date=2025-11-14"

    def measurements_dir(self, zone, sensor_id, ingest_date):
        return f"{self.zone_dir(zone)}/measurements/ingest_date={ingest_date}/sensor_id={sensor_id}"
    
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        folder = self.measurements_dir(zone, sensor_id, ingest_date)
        for page_number, page_data in enumerate(pages_data, start =1):
            path = f"{folder}/page-{page_number}.json"
            #path example: "zone=X/.../sensor_id=Z/page-1.json"
            self.save_json(path, page_data)

    def save_locations_index(self, zone, locations, ingest_date):
        """Save locations index to S3"""
        path = f"{self.metadata_dir(zone, ingest_date)}/locations_index.json"
        self.save_json(path, {"results": locations})
        return True

    def save_sensors_for_location(self, zone, loc_id, sensors, ingest_date):
        """Save sensors for a location to S3"""
        path = f"{self.metadata_dir(zone, ingest_date)}/sensors_loc-{loc_id}.json"
        self.save_json(path, {"results": sensors})
        return True

    def save_sensors_index(self, zone, sensors_idx, ingest_date):
        """Save sensors index to S3"""
        path = f"{self.metadata_dir(zone, ingest_date)}/sensors_index.json"
        self.save_json(path, sensors_idx)
        return True