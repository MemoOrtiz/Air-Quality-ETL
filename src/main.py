"""
OpenAQ ETL Main Script
Simplified entry point using modular architecture

Usage:
    python -m src.main --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
    python -m src.main --zone Monterrey_Metropolitan --from 2025-09-01T00:00:00Z --to 2025-10-15T23:59:59Z
"""

from src.ingestion.openaq.configs.settings import load_env
from src.ingestion.openaq.cli.argument_parser import parse_arguments
from src.ingestion.openaq.pipeline.orchestrator import DataIngestionOrchestrator

def main():
    """Main entry point for OpenAQ ETL process"""
    # Load environment variables
    load_env()
    
    # Parse CLI arguments
    args = parse_arguments()
    
    # Initialize orchestrator
    orchestrator = DataIngestionOrchestrator(
        zones_config_path=args.zones,
        output_dir=args.out_base,
        target_zone=args.zone,
        storage_type=args.storage
    )
    
    # Run the ETL process
    try:
        orchestrator.run_etl(args.dt_from, args.dt_to)
    except KeyboardInterrupt:
        print("\nETL interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()