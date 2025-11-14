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

        json_string = json.dumps(data)
        json_bytes = json_string.encode('utf-8')

        s3_key = f"{self.prefix}/{path}" # example: "bronze/zone=X/file.json"

        self.s3.put_object(
            Bucket = self.bucket,
            Key = s3_key,
            Body = json_bytes,
        )

    def zone_dir(self, zone):
        return f"zone={zone}" 

    def metadata_dir(self, zone, ingest_date):
        return f"{self.zone_dir(zone)}/metadata/ingest_date={ingest_date}"
    # Returns: "zone=Monterrey/metadata/ingest_date=2025-11-14"

    def measurements_pages_dir(self, zone, sensor_id, ingest_date):
        return f"{self.zone_dir(zone)}/measurements/pages/ingest_date={ingest_date}/sensor_id={sensor_id}"
    
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        folder = self.measurements_pages_dir(zone, sensor_id, ingest_date)

        for page_number, page_data in enumerate(pages_data, start =1):
            path = f"{folder}/page-{page_number}.json"
            #path example: "zone=X/.../sensor_id=Z/page-1.json"
            self.save_json(path, page_data)