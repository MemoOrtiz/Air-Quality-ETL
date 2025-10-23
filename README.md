# Air Quality ETL Pipeline 

A comprehensive Extract, Transform, and Load (ETL) system for air quality data from OpenAQ API v3, designed with modular architecture for scalability and flexibility.

##  Features

- **Modular Architecture**: Domain-driven design with separated concerns
- **Dual Storage Strategy**: Raw API responses and event-date organized data
- **Rate Limiting**: Intelligent OpenAQ API rate limiting (60/min, 2000/hour)
- **Geographic Focus**: Optimized for Monterrey Metropolitan Area, Mexico
- **Robust Error Handling**: Comprehensive logging and error management
- **Environment-based Configuration**: Secure API key and configuration management

##  Project Structure

```
src/openaq_ingestion/
├── main.py                    # Entry point
├── cli/
│   └── args_parser.py         # Command-line interface
├── core/
│   ├── config.py              # Environment configuration
│   └── api.py                 # OpenAQ API client with rate limiting
├── data/
│   ├── fetchers.py            # Data extraction functions
│   └── storage/
│       └── local_fs.py        # Local filesystem storage
├── etl/
│   ├── orchestrator.py        # ETL coordination
│   └── zone_processor.py      # Zone-specific processing
└── utils/
    ├── config_loader.py       # Configuration loading utilities
    ├── helpers.py             # General utility functions
    └── printer.py             # Output formatting
```

##  Data Organization

The ETL system creates a well-organized data structure:

```
raw/openaq/Monterrey_ZMM/
├── measurements/
│   ├── pages/ingest_date=2025-10-23/sensor_id=*/
│   │   └── sensor-*_page-*.json          # Raw API responses
│   └── event_date/year=YYYY/month=MM/day=DD/sensor_id=*/
│       └── sensor-*_YYYY-MM-DD.jsonl     # Event-organized data
└── metadata/ingest_date=2025-10-23/
    ├── locations_index.json              # Geographic locations
    ├── sensors_loc-*.json                # Sensors per location
    └── sensors_index.json                # Complete sensor catalog
```

##  Installation

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/MemoOrtiz/Air-Quality-ETL.git
   cd Air-Quality-ETL
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   Create a `.env` file in the project root:
   ```env
   OPENAQ_API_KEY=your_openaq_api_key_here
   OUT_DIR=./raw/openaq
   API_BASE=https://api.openaq.org/v3
   ```

##  Usage

### Basic Usage

```bash
# Extract data for a specific zone and time range
python -m src.openaq_ingestion.main \
  --zone Monterrey_ZMM \
  --from 2025-09-01T00:00:00Z \
  --to 2025-10-15T23:59:59Z
```

### Advanced Usage

```bash
# All zones with custom output directory
python -m src.openaq_ingestion.main \
  --from 2025-09-01T00:00:00Z \
  --to 2025-10-15T23:59:59Z \
  --out ./my_custom_data

# Custom zones configuration
python -m src.openaq_ingestion.main \
  --zones ./custom_zones.json \
  --from 2025-09-01T00:00:00Z \
  --to 2025-10-15T23:59:59Z
```

### Command-Line Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--zone` | Specific zone name (e.g., 'Monterrey_ZMM') | No | All zones |
| `--from` | Start date/time in ISO format | Yes | - |
| `--to` | End date/time in ISO format | Yes | - |
| `--zones` | Path to zones configuration file | No | `src/scripts/zones_config.json` |
| `--out` | Base output directory | No | `./raw_openaq` |

##  Configuration

### Zones Configuration

Edit `src/scripts/zones_config.json` to define geographic zones:

```json
{
  "zones": [
    {
      "name": "Monterrey_ZMM",
      "bbox": [-100.6, 25.5, -99.95, 25.85]
    }
  ]
}
```

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `OPENAQ_API_KEY` | Your OpenAQ API key | Yes |
| `OUT_DIR` | Default output directory | Yes |
| `API_BASE` | OpenAQ API base URL | Yes |

##  Data Output

### Raw Data (Pages)
- **Location**: `measurements/pages/ingest_date=YYYY-MM-DD/sensor_id=*/`
- **Format**: JSON files with complete API responses
- **Purpose**: Backup and debugging

### Processed Data (Event Date)
- **Location**: `measurements/event_date/year=YYYY/month=MM/day=DD/sensor_id=*/`
- **Format**: JSONL files organized by measurement timestamp
- **Purpose**: Efficient querying and analytics

### Metadata
- **locations_index.json**: Geographic locations and their IDs
- **sensors_index.json**: Complete sensor catalog with parameters
- **sensors_loc-*.json**: Sensors grouped by location

##  Architecture Details

### Rate Limiting Strategy
The system implements intelligent rate limiting to respect OpenAQ API limits:
- **Per-minute limit**: 60 requests
- **Hourly limit**: 2000 requests
- **Adaptive delays**: Automatic adjustment based on API headers

### Error Handling
- Graceful handling of API timeouts and errors
- Continuation of processing when individual sensors fail
- Comprehensive error logging and reporting

### Data Quality
- Validation of measurement timestamps
- Handling of missing or invalid data
- Duplicate detection and prevention


## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments

- [OpenAQ](https://openaq.org/) for providing comprehensive air quality data
- The Monterrey Metropolitan Area environmental monitoring community

##  Support

For questions, issues, or contributions, please open an issue in the GitHub repository.
