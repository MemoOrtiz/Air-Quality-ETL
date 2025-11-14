#ABC its Abstract Base Class
# abstractmethod decorator forces subclasses to implement the methods
from abc import ABC, abstractmethod

class StorageInterface(ABC):
    @abstractmethod
    def save_json(self,path: str, data:dict):
        """ Save a dictionary as a JSON file"""
        pass

    @abstractmethod
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        """ Save raw measurements data (Bronze)"""
        pass
