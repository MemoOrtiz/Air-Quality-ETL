# src/openaq_ingestion/etl/orchestrator.py
from .zone_processor import ZoneProcessor
from ..data.storage.local_fs import RawLocal
from ..utils.config_loader import load_zones_config
from ..utils.printer import print_header, print_final_summary
from ..utils.helpers import ingest_date_utc

class DataIngestionOrchestrator:
    """Main ETL orchestration logic"""
    
    def __init__(self, zones_config_path: str, output_dir: str, target_zone: str = None):
        self.zones_config_path = zones_config_path
        self.output_dir = output_dir
        self.target_zone = target_zone
        self.storage = RawLocal(base=output_dir)
        self.processor = ZoneProcessor(self.storage)
    
    def run_etl(self, dt_from: str, dt_to: str) -> bool:
        """Run complete ETL process"""
        print_header()
        
        # Load and filter zones
        zones = self._load_and_filter_zones()
        
        # Initialize global statistics
        total_stats = self._initialize_global_stats(zones)
        ingest_date = ingest_date_utc()
        
        # Process each zone
        try:
            for zone in zones:
                zone_stats = self.processor.process_zone(
                    zone_name=zone["name"],
                    bbox=tuple(zone["bbox"]),
                    dt_from=dt_from,
                    dt_to=dt_to,
                    ingest_date=ingest_date
                )
                
                # Accumulate statistics
                self._accumulate_stats(total_stats, zone_stats)
            
            # Generate final report
            success = self._print_final_report(total_stats, zones, ingest_date)
            return success
            
        except KeyboardInterrupt:
            print("\nETL interrupted by user")
            return False
        except Exception as e:
            print(f"\nFatal error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _load_and_filter_zones(self) -> list:
        """Load zones configuration and filter if specific zone requested"""
        zones = load_zones_config(self.zones_config_path)
        
        # Filter specific zone if requested
        if self.target_zone:
            zones = [z for z in zones if z["name"] == self.target_zone]
            if not zones:
                print(f"Error: Zone '{self.target_zone}' not found in {self.zones_config_path}")
                available_zones = [z["name"] for z in load_zones_config(self.zones_config_path)]
                print(f"Available zones: {', '.join(available_zones)}")
                exit(1)
        
        print(f"Zones to process: {len(zones)}")
        for zone in zones:
            print(f"    • {zone['name']}: {zone['bbox']}")
        print()
        
        return zones
    
    def _initialize_global_stats(self, zones: list) -> dict:
        """Initialize global statistics structure"""
        return {
            'zones': len(zones),
            'locations': 0,
            'sensors': 0,
            'measurements': 0,
            'errors': 0
        }
    
    def _accumulate_stats(self, total_stats: dict, zone_stats: dict) -> None:
        """Accumulate zone statistics into global statistics"""
        for key in ['locations', 'sensors', 'measurements', 'errors']:
            total_stats[key] += zone_stats[key]
    
    def _print_final_report(self, total_stats: dict, zones: list, ingest_date: str) -> bool:
        """Print final ETL report"""
        print_final_summary(total_stats, zones, self.output_dir, ingest_date)
        
        if total_stats['errors'] > 0:
            print(f"⚠️  Process completed with {total_stats['errors']} errors")
            return False
        else:
            print("✅ Process completed successfully")
            return True