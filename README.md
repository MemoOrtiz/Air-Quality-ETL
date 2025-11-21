# Air Quality ETL Pipeline

A production-ready Extract, Transform, and Load (ETL) system for air quality data from OpenAQ API v3. Built with a modular, interface-driven architecture supporting multiple storage backends (Local FileSystem and AWS S3) using the Medallion Architecture pattern (Bronze → Silver → Gold).

---

## Table of Contents

1. [Features](#features)
2. [Architecture Overview](#architecture-overview)
3. [Project Structure Explained](#project-structure-explained)
4. [How Components Work Together](#how-components-work-together)
   - [Configuration Chain: JSON → .env → Config](#1-configuration-chain-json--env--config)
   - [Storage Interface Pattern](#2-storage-interface-pattern)
   - [Orchestrator: The Central Coordinator](#3-orchestrator-the-central-coordinator)
   - [Zone Processor: Storage-Agnostic Worker](#4-zone-processor-storage-agnostic-worker)
5. [Execution Flow (Step-by-Step)](#execution-flow-step-by-step)
6. [Data Organization (Medallion Architecture)](#data-organization-medallion-architecture)
7. [Configuration Deep Dive](#configuration-deep-dive)
8. [Storage Interface: The Power of Abstraction](#storage-interface-the-power-of-abstraction)
9. [Installation & Setup](#installation--setup)
10. [Usage Examples](#usage-examples)
11. [Advanced Configuration](#advanced-configuration)
12. [Testing & Validation](#testing--validation)
13. [Contributing](#contributing)
14. [License](#license)
15. [Contact](#contact)

---

##  Features

- **Multi-Storage Backend**: Pluggable storage architecture supporting Local FileSystem and AWS S3
- **Interface-Driven Design**: Clean abstraction layer enabling easy addition of new storage backends (Azure Blob, GCS, etc.)
- **Medallion Architecture**: Bronze layer implementation for immutable data lake ingestion
- **Environment-Based Configuration**: Flexible `.env` configuration with automatic storage detection
- **Rate Limiting**: Intelligent OpenAQ API rate limiting (60/min, 2000/hour)
- **Geographic Zones**: Configurable bounding box filtering for multiple metropolitan areas
- **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- **Production Ready**: Tested with real-world data from Mexican metropolitan areas

---

## Architecture Overview

### **High-Level Data Flow**

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   CLI Args   │────▶│ Orchestrator │────▶│Zone Processor│────▶│   Storage    │
│ (args_parser)│     │              │     │              │     │  (Local/S3)  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                             │                     │                     │
                             ▼                     ▼                     ▼
                      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
                      │zones_config │      │   Fetchers  │      │ Bronze Data │
                      │    .json    │      │  (API Calls)│      │   (Bronze)  │
                      └─────────────┘      └─────────────┘      └─────────────┘
```

### **Component Interaction Flow**

1. **Configuration Loading** (`zones_config.json` → `config_loader.py`)
2. **Environment Setup** (`.env` → `config.py`)
3. **Storage Selection** (`config.py` → `orchestrator.py` → `StorageInterface`)
4. **ETL Execution** (`orchestrator.py` → `zone_processor.py` → `fetchers.py` → `storage`)

---

## Project Structure

```
data-project/
├── src/
│   ├── main.py                           # Entry point
│   ├── ingestion/
│   │   └── openaq/
│   │       ├── cli/
│   │       │   └── argument_parser.py
│   │       ├── configs/
│   │       │   ├── settings.py
│   │       │   └── zones_config.json
│   │       ├── fetchers/
│   │       │   ├── http_client.py
│   │       │   └── fetchers.py
│   │       ├── storage/
│   │       │   ├── storage_interface.py
│   │       │   ├── local_filesystem.py
│   │       │   └── s3_storage.py
│   │       ├── pipeline/
│   │       │   ├── orchestrator.py
│   │       │   └── zone_processor.py
│   │       └── utils/
│   │           ├── config_loader.py
│   │           └── helpers.py
│   ├── transformation/
│   └── aggregation/
├── bronze/                                  # Local data output (Bronze layer)
├── tests/
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

---

## How Components Work Together

### **1. Configuration Chain: JSON → .env → Config**

#### **Step 1: Zones Configuration (`zones_config.json`)**

Defines geographic areas to process:

```json
{
  "zones": [
    { 
      "name": "Monterrey_Metropolitan", 
      "bbox": [-100.60, 25.50, -99.95, 25.85] 
    },
    {
      "name": "Guadalajara_Metropolitan",
      "bbox": [-103.50, 20.50, -103.20, 20.80]
    }
  ]
}
```

**Loaded by:** `utils/config_loader.py` → Used by: `orchestrator.py`

#### **Step 2: Environment Variables (`.env`)**

Provides runtime configuration:

```bash
# OpenAQ API
OPENAQ_API_KEY=your_api_key_here
API_BASE=https://api.openaq.org/v3

# Local Storage
OUT_DIR=./bronze

# AWS S3 Storage (optional)
AWS_S3_BUCKET_NAME=your-bucket-name
AWS_S3_PREFIX=bronze
AWS_ACCESS_KEY_ID=XXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxx
AWS_DEFAULT_REGION=us-east-1
```

**Loaded by:** `configs/settings.py` using `python-dotenv`

#### **Step 3: Config Module (`configs/settings.py`)**

Exposes configuration through functions:

```python
def storage_mode():
    """Auto-detect storage: 's3' if AWS_S3_BUCKET_NAME exists, else 'local'"""
    return "s3" if s3_bucket() else "local"

def s3_bucket():
    return os.getenv("AWS_S3_BUCKET_NAME")

def out_dir():
    return os.getenv("OUT_DIR")
```

**Used by:** `pipeline/orchestrator.py` to initialize storage

---

### **2. Storage Interface Pattern**

#### **The Abstract Interface** (`storage/storage_interface.py`)

Defines the contract that ALL storage backends must implement:

```python
from abc import ABC, abstractmethod

class StorageInterface(ABC):
    @abstractmethod
    def save_json(self, path: str, data: dict):
        """Save a dictionary as JSON"""
        pass

    @abstractmethod
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        """Save raw measurements data (Bronze layer)"""
        pass
    
    # Additional methods: save_locations_index, save_sensors_for_location, etc.
```

**Why this matters:**
-  **Polymorphism**: `ZoneProcessor` doesn't care if it's Local or S3
-  **Extensibility**: Adding Azure Blob Storage = creating new class implementing interface
-  **Testability**: Easy to mock storage for unit tests

#### **Local Implementation** (`storage/local_filesystem.py`)

```python
class LocalStorage(StorageInterface):
    def __init__(self, base="./raw"):
        self.base = base

    def save_json(self, path: str, data: dict):
        ensure_dir(os.path.dirname(path))
        with open(path, "w") as f:
            json.dump(data, f, ensure_ascii=False)

    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        folder = self.measurements_pages_dir(zone, sensor_id, ingest_date)
        for page_num, page_data in enumerate(pages_data, 1):
            file_path = os.path.join(folder, f"page-{page_num}.json")
            self.save_json(file_path, page_data)
```

#### **S3 Implementation** (`storage/s3_storage.py`)

```python
class S3Storage(StorageInterface):
    def __init__(self, bucket_name, prefix="bronze"):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.prefix = prefix

    def save_json(self, path: str, data: dict):
        s3_key = f"{self.prefix}/{path}"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )

    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        folder = self.measurements_pages_dir(zone, sensor_id, ingest_date)
        for page_num, page_data in enumerate(pages_data, 1):
            path = f"{folder}/page-{page_num}.json"
            self.save_json(path, page_data)
```

**Key Difference:**
- Local: Uses `os.path` and `open()`
- S3: Uses `boto3` and `put_object()`
- **Same interface** = `ZoneProcessor` doesn't need to know!

---

### **3. Orchestrator: The Central Coordinator** (`pipeline/orchestrator.py`)

The orchestrator ties everything together:

```python
class DataIngestionOrchestrator:
    def __init__(self, zones_config_path, output_dir, target_zone=None, storage_type=None):
        self.zones_config_path = zones_config_path
        self.output_dir = output_dir
        self.storage_type = storage_type
        
        # Initialize storage based on config
        self.storage = self._initialize_storage()
        
        # Pass storage to zone processor
        self.processor = ZoneProcessor(self.storage)
    
    def _initialize_storage(self):
        # Determine mode: explicit (--storage flag) or auto-detect (.env)
        mode = self.storage_type or storage_mode()
        
        if mode == "s3":
            bucket = s3_bucket()
            prefix = s3_prefix()
            return S3Storage(bucket_name=bucket, prefix=prefix)  # ← S3 implementation
        else:
            return LocalStorage(base=self.output_dir)  # ← Local implementation
```

**Flow:**
1. Read `.env` → `config.py` functions
2. Decide: S3 or Local?
3. Instantiate correct storage class
4. Pass to `ZoneProcessor`

---

### **4. Zone Processor: Storage-Agnostic Worker** (`pipeline/zone_processor.py`)

Processes individual geographic zones without knowing storage details:

```python
class ZoneProcessor:
    def __init__(self, storage: StorageInterface):  # Accepts interface, not concrete class
        self.storage = storage
    
    def extract_zone_data(self, zone_name, bbox, dt_from, dt_to, ingest_date):
        # 1. Fetch locations from API
        locations = fetch_locations_bbox(bbox)
        
        # 2. Save using storage interface (could be Local or S3!)
        self.storage.save_locations_index(zone_name, locations, ingest_date)
        
        # 3. Fetch sensors
        sensors = fetch_sensors_for_location(loc_id)
        
        # 4. Save sensors
        self.storage.save_sensors_for_location(zone_name, loc_id, sensors, ingest_date)
        
        # 5. Fetch measurements
        pages_data = fetch_measurements_for_sensor_raw(sensor_id, dt_from, dt_to)
        
        # 6. Save raw data (Bronze layer)
        self.storage.save_measurements_raw(zone_name, sensor_id, pages_data, ingest_date)
```

**Polymorphism in Action:**
- `self.storage.save_json(...)` calls:
  - `LocalStorage.save_json()` if local mode
  - `S3Storage.save_json()` if S3 mode
- **Same code**, different behavior!

---

## Execution Flow (Step-by-Step)

```
1. User runs: python -m src.main --storage s3 --from ... --to ...
                                    ↓
2. main.py → load_env() → loads .env variables
                                    ↓
3. main.py → parse_arguments() → gets CLI args (--storage s3)
                                    ↓
4. main.py → DataIngestionOrchestrator(storage_type="s3")
                                    ↓
5. orchestrator._initialize_storage():
   - Reads: storage_type="s3" (from CLI)
   - Reads: AWS_S3_BUCKET_NAME from .env (via configs/settings.py)
   - Creates: S3Storage(bucket="datalake-openaq", prefix="bronze")
   - Returns: storage instance
                                    ↓
6. orchestrator creates: ZoneProcessor(storage=S3Storage instance)
                                    ↓
7. orchestrator.run_etl():
   - Loads zones_config.json → config_loader.py
   - For each zone:
       ↓
8. zone_processor.extract_zone_data():
   - Calls: fetch_locations_bbox() → API request
   - Calls: self.storage.save_locations_index() → S3Storage.save_json()
      → boto3.put_object() → uploads to s3://datalake-openaq/bronze/zone=X/metadata/...
   - Calls: fetch_sensors_for_location() → API request
   - Calls: self.storage.save_sensors_for_location() → S3Storage.save_json()
   - Calls: fetch_measurements_for_sensor_raw() → API requests (paginated)
   - Calls: self.storage.save_measurements_raw() → S3Storage.save_json()
      → Multiple files uploaded to S3
                                    ↓
9. Repeat for all zones
                                    ↓
10. Print final summary → Success!
```

---

## Data Organization (Medallion Architecture)

### **Bronze Layer (Immutable Source Data)**

The system organizes data following the **Medallion Architecture** pattern:

**Local Storage Structure:**
```
bronze/
└── zone={zone_name}/
    ├── measurements/
    │   └── pages/
    │       └── ingest_date={YYYY-MM-DD}/
    │           └── sensor_id={id}/
    │               ├── page-1.json
    │               ├── page-2.json
    │               └── page-N.json
    └── metadata/
        └── ingest_date={YYYY-MM-DD}/
            ├── locations_index.json
            ├── sensors_index.json
            └── sensors_loc-{location_id}.json
```

**Example (Guadalajara_Metropolitan zone):**
```
bronze/
└── zone=Guadalajara_Metropolitan/
    ├── measurements/
    │   └── pages/
    │       └── ingest_date=2025-11-14/
    │           ├── sensor_id=22803/
    │           │   └── page-1.json
    │           ├── sensor_id=22932/
    │           │   ├── page-1.json
    │           │   └── page-2.json
    │           └── sensor_id=23112/
    │               └── page-1.json
    └── metadata/
        └── ingest_date=2025-11-14/
            ├── locations_index.json
            ├── sensors_index.json
            ├── sensors_loc-10536.json
            ├── sensors_loc-10549.json
            └── sensors_loc-7719.json
```

**S3 Storage Structure:**
```
s3://{bucket_name}/{prefix}/
└── zone={zone_name}/
    ├── measurements/pages/ingest_date={YYYY-MM-DD}/sensor_id={id}/page-N.json
    └── metadata/ingest_date={YYYY-MM-DD}/{filename}.json
```

**Partitioning Strategy:**
- `zone=`: Geographic area (Monterrey_Metropolitan, Guadalajara_Metropolitan, CDMX_Metropolitan)
- `ingest_date=`: When data was ingested (enables incremental processing)
- `sensor_id=`: Individual sensor identifier

**Why Bronze Layer?**
- **Raw, immutable**: Exact API responses preserved
- **Schema-free**: No transformations, future-proof
- **Replayable**: Can rebuild Silver/Gold layers from Bronze
- **Audit trail**: Full data lineage

---

### **JSON File Examples**

The following examples show the actual content structure of the JSON files stored in the Bronze layer:

**Locations Index (`metadata/ingest_date=2025-11-14/locations_index.json`):**
```json
{
  "locations": [
    {
      "id": 10666,
      "name": "Obispado",
      "coordinates": {
        "latitude": 25.6864,
        "longitude": -100.3364
      }
    },
    {
      "id": 10710,
      "name": "Santa Catarina",
      "coordinates": {
        "latitude": 25.6742,
        "longitude": -100.4589
      }
    }
  ]
}
```

**Sensors for Location (`metadata/ingest_date=2025-11-14/sensors_loc-10666.json`):**
```json
{
  "location_id": 10666,
  "sensors": [
    {
      "id": 22803,
      "name": "PM2.5",
      "parameter": {
        "id": 2,
        "name": "pm25",
        "units": "µg/m³"
      }
    },
    {
      "id": 22804,
      "name": "PM10",
      "parameter": {
        "id": 1,
        "name": "pm10",
        "units": "µg/m³"
      }
    }
  ]
}
```

**Measurements Page (`measurements/pages/ingest_date=2025-11-14/sensor_id=22803/page-1.json`):**
```json
{
  "meta": {
    "found": 350,
    "limit": 1000,
    "page": 1
  },
  "results": [
    {
      "period": {
        "label": "2025-10-01 00:00:00+00 - 2025-10-01 01:00:00+00",
        "datetimeFrom": {
          "utc": "2025-10-01T00:00:00Z",
          "local": "2025-09-30T18:00:00-06:00"
        },
        "datetimeTo": {
          "utc": "2025-10-01T01:00:00Z",
          "local": "2025-09-30T19:00:00-06:00"
        }
      },
      "value": 42.5,
      "parameter": {
        "id": 2,
        "name": "pm25",
        "units": "µg/m³",
        "displayName": "PM2.5"
      },
      "coordinates": {
        "latitude": 25.6864,
        "longitude": -100.3364
      }
    },
    {
      "period": {
        "label": "2025-10-01 01:00:00+00 - 2025-10-01 02:00:00+00",
        "datetimeFrom": {
          "utc": "2025-10-01T01:00:00Z",
          "local": "2025-09-30T19:00:00-06:00"
        },
        "datetimeTo": {
          "utc": "2025-10-01T02:00:00Z",
          "local": "2025-09-30T20:00:00-06:00"
        }
      },
      "value": 38.2,
      "parameter": {
        "id": 2,
        "name": "pm25",
        "units": "µg/m³",
        "displayName": "PM2.5"
      },
      "coordinates": {
        "latitude": 25.6864,
        "longitude": -100.3364
      }
    }
  ]
}
```

**Key Points:**
- **Metadata files**: Contain location and sensor catalog information
- **Measurements files**: Raw paginated API responses with hourly readings
- **Timestamps**: Include both UTC and local time for each measurement
- **Complete context**: Each measurement includes coordinates, parameter details, and period information
- **Unmodified**: Exact API response structure preserved for future reprocessing

---

## Installation & Setup

### **Prerequisites**
- Python 3.11+
- (Optional) AWS Account with S3 access

### **Setup**

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
   
   **For Local Storage:**
   ```env
   # OpenAQ API
   OPENAQ_API_KEY=your_openaq_api_key_here
   API_BASE=https://api.openaq.org/v3
   
   # Local Storage
   OUT_DIR=./bronze
   ```
   
   **For S3 Storage:**
   ```env
   # OpenAQ API
   OPENAQ_API_KEY=your_openaq_api_key_here
   API_BASE=https://api.openaq.org/v3
   
   # AWS S3 Storage
   AWS_S3_BUCKET_NAME=your-bucket-name
   AWS_S3_PREFIX=bronze
   AWS_ACCESS_KEY_ID=AKIAXXXXXXXXX
   AWS_SECRET_ACCESS_KEY=xxxxx
   AWS_DEFAULT_REGION=us-east-1
   ```

---

## Usage Examples

### **Basic Usage (Auto-Detect Storage)**

```bash
# System auto-detects S3 if AWS_S3_BUCKET_NAME is in .env
python -m src.main \
  --zone Monterrey_Metropolitan \
  --from 2025-09-01T00:00:00Z \
  --to 2025-10-15T23:59:59Z
```

### **Explicit Storage Selection**

```bash
# Force local storage (even if S3 configured)
python -m src.main \
  --storage local \
  --zone Guadalajara_Metropolitan \
  --from 2025-10-01T00:00:00Z \
  --to 2025-10-31T23:59:59Z

# Force S3 storage
python -m src.main \
  --storage s3 \
  --zone CDMX_Metropolitan \
  --from 2025-11-01T00:00:00Z \
  --to 2025-11-30T23:59:59Z
```

### **Process All Zones**

```bash
# Extract all zones defined in zones_config.json
python -m src.main \
  --from 2025-09-01T00:00:00Z \
  --to 2025-10-15T23:59:59Z
```

### **Custom Zones Configuration**

```bash
# Use custom zones configuration file
python -m src.main \
  --zones ./custom_zones.json \
  --from 2025-09-01T00:00:00Z \
  --to 2025-10-15T23:59:59Z
```

### **Command-Line Options**

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--storage` | Storage backend: `local` or `s3` | No | Auto-detect from `.env` |
| `--zone` | Specific zone name (e.g., 'Monterrey_Metropolitan') | No | All zones |
| `--from` | Start date/time in ISO format | Yes | - |
| `--to` | End date/time in ISO format | Yes | - |
| `--zones` | Path to zones configuration file | No | `src/ingestion/openaq/configs/zones_config.json` |
| `--out` | Base output directory (local storage) | No | `./bronze` |

---

## Advanced Configuration

### **Zones Configuration (`zones_config.json`)**

Defines geographic areas to extract data from using bounding boxes:

```json
{
  "zones": [
    {
      "name": "Monterrey_Metropolitan",
      "bbox": [-100.6, 25.5, -99.95, 25.85]
    },
    {
      "name": "Guadalajara_Metropolitan",
      "bbox": [-103.5, 20.5, -103.2, 20.8]
    },
    {
      "name": "CDMX_Metropolitan",
      "bbox": [-99.35, 19.25, -98.95, 19.55]
    }
  ]
}
```

**Bounding Box Format:** `[west_longitude, south_latitude, east_longitude, north_latitude]`

**Use Cases:**
- Metropolitan areas
- Air quality monitoring regions
- Custom geographic boundaries

### **Environment Variables Reference**

| Variable | Description | Required | Example |
|----------|-------------|----------|---------|
| `OPENAQ_API_KEY` | OpenAQ API v3 authentication key | Yes | `.....` |
| `API_BASE` | OpenAQ API base URL | Yes | `https://api.openaq.org/v3` |
| `OUT_DIR` | Local storage base directory | Local only | `./bronze` |
| `AWS_S3_BUCKET_NAME` | S3 bucket name for data lake | S3 only | `datalake-openaq` |
| `AWS_S3_PREFIX` | S3 key prefix (medallion layer) | S3 only | `bronze` |
| `AWS_ACCESS_KEY_ID` | AWS IAM credentials | S3 only | `XXXXXXXXX` |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret | S3 only | `xxxxx` |
| `AWS_DEFAULT_REGION` | AWS region | S3 only | `us-east-1` |

---

## Testing & Validation

### **Pre-Execution Checks**

Before running the ETL, verify your configuration:

```python
# Activate virtual environment
source .venv/bin/activate

# Test 1: Environment variables loaded
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print('S3 Bucket:', os.getenv('AWS_S3_BUCKET_NAME'))"

# Test 2: Storage mode detection
python -c "from src.ingestion.openaq.configs.settings import storage_mode; print('Storage mode:', storage_mode())"

# Test 3: AWS connectivity (S3 only)
python -c "import boto3; s3=boto3.client('s3'); print('S3 buckets:', [b['Name'] for b in s3.list_buckets()['Buckets']])"
```

### **Dry Run**

Test with a small date range first:

```bash
# Test single day
python -m src.main \
  --storage local \
  --zone Monterrey_Metropolitan \
  --from 2025-10-01T00:00:00Z \
  --to 2025-10-01T23:59:59Z
```

---

## Contributing

Contributions are welcome! Areas for improvement:

- **Additional Storage Backends**: Azure Blob Storage, Google Cloud Storage
- **Silver/Gold Layers**: Data transformations and aggregations
- **Data Quality**: Validation and cleansing pipelines
- **Monitoring**: Metrics and alerting for ETL jobs
- **Testing**: Unit and integration tests

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Contact

For questions or support, please open an issue on GitHub.
