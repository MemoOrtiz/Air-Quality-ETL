# Air Quality ETL Pipeline

An Extract, Transform, and Load (ETL) system for air quality data from the OpenAQ API v3, built with a modular, interface-driven architecture that supports multiple storage backends (local filesystem and AWS S3) and follows the Medallion Architecture pattern (Bronze → Silver → Gold).

**Current status:** the **Bronze** ingestion layer is complete and functional. **Silver** (transformation) and **Gold** (aggregation) are designed but **not implemented yet** — see the [Roadmap](#roadmap).

---

## Table of Contents

1. [Features](#features)
2. [Roadmap](#roadmap)
3. [Architecture Overview](#architecture-overview)
4. [Project Structure](#project-structure)
5. [How Components Work Together](#how-components-work-together)
   - [Configuration Chain: JSON to .env to Config](#1-configuration-chain-json-to-env-to-config)
   - [Storage Interface Pattern](#2-storage-interface-pattern)
   - [Orchestrator: The Central Coordinator](#3-orchestrator-the-central-coordinator)
   - [Zone Processor: Storage-Agnostic Worker](#4-zone-processor-storage-agnostic-worker)
6. [Execution Flow](#execution-flow)
7. [Data Organization](#data-organization)
8. [Sample Data Included](#sample-data-included)
9. [Rate Limiting and Performance](#rate-limiting-and-performance)
10. [Installation and Setup](#installation-and-setup)
11. [Usage](#usage)
12. [Configuration Reference](#configuration-reference)
13. [Known Limitations and Design Decisions](#known-limitations-and-design-decisions)
14. [Further Documentation](#further-documentation)
15. [Contributing](#contributing)
16. [License](#license)
17. [Contact](#contact)

---

## Features

- **Multi-storage backend**: pluggable storage supporting the local filesystem and AWS S3
- **Interface-driven design**: an abstraction layer that makes adding new backends (Azure Blob, GCS, MinIO) a matter of writing one class
- **Medallion architecture**: the Bronze layer is implemented for immutable data lake ingestion; Silver and Gold are planned (see [Roadmap](#roadmap))
- **Hive-style partitioning**: `zone=/ingest_date=/sensor_id=` — read natively by Athena, Spark, Glue and Databricks with no extra configuration
- **Ingest/event date separation**: `ingest_date` records when data was fetched, keeping the door open to backfills and reprocessing
- **Environment-based configuration**: `.env` driven, with automatic storage detection and a CLI override
- **Rate limiting**: reactive, header-based throttling for the OpenAQ API (60/min, 2000/hour) with retry on HTTP 429
- **Geographic zones**: configurable bounding-box filtering for multiple metropolitan areas
- **Active-sensor filtering**: sensors whose activity period does not overlap the requested range are skipped before any measurement request is issued

---

## Roadmap

### Status by layer

| Layer | Status | Notes |
|---|---|---|
| **Bronze** (ingestion) | Functional | OpenAQ to raw JSON, local/S3, Hive-style partitioning |
| **Silver** (transformation) | Not started | `src/transformation/` does not exist yet |
| **Gold** (aggregation) | Not started | `src/aggregation/` does not exist yet |
| Tests | Not started | No `tests/` directory yet |
| Docker | Not started | No Dockerfiles yet |
| CI/CD | Not started | No `.github/workflows/` yet |
| Orchestration | Not started | Manual CLI execution |

### What comes next

1. **Storage read methods.** The current contract only writes. Adding `read_bytes`, `save_bytes`, `list_paths` and `exists` is what unblocks Silver.
2. **`src/common/paths.py`.** A shared, neutral path contract that Bronze, Silver and Gold all import from.
3. **Silver.** Read Bronze, flatten, clean, deduplicate, and write Parquet partitioned by `event_date`.
4. **Containers and CI.** Per-stage `requirements/`, one Dockerfile per stage, tests running on every push.
5. **Orchestration.** GitHub Actions with a daily schedule; containerized Airflow once Gold exists and there is a real dependency graph.
6. **Gold.** Star schema and aggregates by parameter, zone, day and hour.

---

## Architecture Overview

### High-level data flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   CLI args   │────▶│ Orchestrator │────▶│Zone Processor│────▶│   Storage    │
│(arg_parser)  │     │              │     │              │     │  (Local/S3)  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                             │                     │                     │
                             ▼                     ▼                     ▼
                      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
                      │zones_config │      │  Fetchers   │      │ Bronze data │
                      │    .json    │      │ (API calls) │      │  (raw JSON) │
                      └─────────────┘      └─────────────┘      └─────────────┘
```

### The design principle

The pipeline stages communicate through **data at rest**, not through function calls. Bronze writes to `bronze/`; Silver will read from `bronze/` and write to `silver/`. No stage imports another. That constraint is what will let each stage run on its own schedule, in its own container, and be tested without executing the ones before it.

### Component interaction

1. **Configuration loading** — `zones_config.json` via `utils/config_loader.py`
2. **Environment setup** — `.env` via `configs/settings.py`
3. **Storage selection** — `settings.py` to `pipeline/orchestrator.py` to a `StorageInterface` implementation
4. **ETL execution** — `orchestrator.py` to `zone_processor.py` to `fetchers.py` to storage

---

## Project Structure

```
Air-Quality-ETL/
├── src/
│   ├── main.py                          # Bronze entry point
│   └── ingestion/
│       └── openaq/
│           ├── cli/
│           │   ├── argument_parser.py   # CLI definition
│           │   └── output_formatter.py  # console reporting
│           ├── configs/
│           │   ├── settings.py          # reads .env (python-dotenv)
│           │   └── zones_config.json    # zones + bounding boxes
│           ├── fetchers/
│           │   ├── http_client.py       # HTTP client, rate limiting, retries
│           │   └── fetchers.py          # OpenAQ endpoint wrappers
│           ├── storage/
│           │   ├── storage_interface.py # the ABC contract
│           │   ├── local_filesystem.py  # LocalStorage
│           │   └── s3_storage.py        # S3Storage (boto3)
│           ├── pipeline/
│           │   ├── orchestrator.py      # DataIngestionOrchestrator
│           │   └── zone_processor.py    # ZoneProcessor
│           └── utils/
│               ├── config_loader.py
│               └── helpers.py
├── bronze/                              # local Bronze output
│   └── zone=Guadalajara_Metropolitan/   # committed sample dataset
│       ├── measurements/
│       └── metadata/
├── docs/                                # project documentation
├── .env                                 # not committed
├── .gitignore
├── LICENSE
├── requirements.txt
└── README.md
```

`src/transformation/` (Silver) and `src/aggregation/` (Gold) are **not** in this listing because they do not exist yet. They will be created in the phases described in the [Roadmap](#roadmap).

---

## How Components Work Together

### 1. Configuration Chain: JSON to .env to Config

#### Step 1: zones configuration (`configs/zones_config.json`)

Defines the geographic areas to process:

```json
{
  "zones": [
    { "name": "Monterrey_Metropolitan",   "bbox": [-100.60, 25.50, -99.95, 25.85] },
    { "name": "Guadalajara_Metropolitan", "bbox": [-103.50, 20.50, -103.20, 20.80] },
    { "name": "CDMX_Metropolitan",        "bbox": [-99.35, 19.15, -98.95, 19.65] }
  ]
}
```

Bounding box format: `[west_longitude, south_latitude, east_longitude, north_latitude]`.

**Loaded by** `utils/config_loader.py`, **used by** `pipeline/orchestrator.py`.

#### Step 2: environment variables (`.env`)

```bash
# OpenAQ API
OPENAQ_API_KEY=your_api_key_here
API_BASE=https://api.openaq.org/v3

# Local storage
OUT_DIR=./bronze

# AWS S3 storage (optional)
AWS_S3_BUCKET_NAME=your-bucket-name
AWS_S3_PREFIX=bronze
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
```

**Loaded by** `configs/settings.py` using `python-dotenv`.

#### Step 3: config module (`configs/settings.py`)

```python
def storage_mode():
    """'s3' if AWS_S3_BUCKET_NAME exists, otherwise 'local'"""
    return "s3" if s3_bucket() else "local"

def s3_bucket():
    return os.getenv("AWS_S3_BUCKET_NAME")

def out_dir():
    """Raises if OUT_DIR is missing, to force explicit configuration"""
    ...
```

Precedence: the **`--storage` flag wins over autodetection** from `.env`.

Note that `api_base()`, `api_headers()` and `out_dir()` all raise a `ValueError` with a remediation hint when their variable is missing. Configuration is explicit by design; there are no silent defaults for those three.

---

### 2. Storage Interface Pattern

#### The abstract contract (`storage/storage_interface.py`)

```python
from abc import ABC, abstractmethod

class StorageInterface(ABC):
    @abstractmethod
    def save_json(self, path: str, data: dict):
        """Save a dictionary as a JSON file"""

    @abstractmethod
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        """Save raw measurements data (Bronze)"""
```

The ABC declares **exactly two** abstract methods. `LocalStorage` and `S3Storage` additionally provide `save_locations_index`, `save_sensors_by_location`, `save_sensors_index` and a set of path helpers, but those live **only in the concrete classes**, not in the contract. `ZoneProcessor` calls some of them, so today it works by duck-typing.

This is a known gap, deliberately documented rather than hidden: a new backend could satisfy the ABC and still fail at runtime for lacking methods the interface never demanded. The planned fix is to trim the contract to domain-agnostic primitives (`save_bytes`, `read_bytes`, `list_paths`, `exists`, with `save_json` as a helper) and move the OpenAQ-specific methods up into the ingestion layer. See [Known Limitations](#known-limitations-and-design-decisions).

#### Local implementation (`storage/local_filesystem.py`)

```python
class LocalStorage(StorageInterface):
    def __init__(self, base="./bronze"):
        self.base = base

    def save_json(self, path: str, data: dict):
        ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date):
        folder = self.measurements_dir(zone, sensor_id, ingest_date)
        for page_num, page_data in enumerate(pages_data, 1):
            self.save_json(os.path.join(folder, f"page-{page_num}.json"), page_data)
```

#### S3 implementation (`storage/s3_storage.py`)

```python
class S3Storage(StorageInterface):
    def __init__(self, bucket_name, prefix="bronze"):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name
        self.prefix = prefix

    def save_json(self, path: str, data: dict):
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}/{path}",
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
```

**The difference is confined to I/O:** local uses `os.path` and `open()`, S3 uses `boto3.put_object()`. Everything above the storage layer is unchanged.

---

### 3. Orchestrator: The Central Coordinator

`pipeline/orchestrator.py` decides which backend to build and wires it into the processor:

```python
class DataIngestionOrchestrator:
    def __init__(self, zones_config_path, output_dir, target_zone=None, storage_type=None):
        self.storage = self._initialize_storage()
        self.processor = ZoneProcessor(self.storage)

    def _initialize_storage(self):
        mode = self.storage_type or storage_mode()   # CLI flag wins over .env
        if mode == "s3":
            return S3Storage(bucket_name=s3_bucket(), prefix=s3_prefix())
        return LocalStorage(base=self.output_dir)
```

---

### 4. Zone Processor: Storage-Agnostic Worker

`pipeline/zone_processor.py` receives the **interface**, never a concrete class:

```python
class ZoneProcessor:
    def __init__(self, storage: StorageInterface):
        self.storage = storage

    def extract_zone_data(self, zone_name, bbox, dt_from, dt_to, ingest_date):
        locations = fetch_locations_bbox(bbox)
        self.storage.save_locations_index(zone_name, locations, ingest_date)

        sensors = fetch_sensors_by_location(loc_id)
        self.storage.save_sensors_by_location(zone_name, loc_id, sensors, ingest_date)

        pages_data = fetch_measurements_for_sensor_raw(sensor_id, dt_from, dt_to)
        self.storage.save_measurements_raw(zone_name, sensor_id, pages_data, ingest_date)
```

It also runs `_filter_active_sensors()`: before requesting any measurements, sensors whose `datetimeFirst`/`datetimeLast` range does not overlap the requested window are dropped. In the committed sample dataset this reduces 147 catalog sensors to the 77 that are actually queried — a meaningful saving against the API rate limit.

---

## Execution Flow

```
1. python -m src.main --storage s3 --from ... --to ...
                          ↓
2. main.py loads .env via load_env()
                          ↓
3. parse_arguments() reads the CLI flags
                          ↓
4. DataIngestionOrchestrator(storage_type="s3")
                          ↓
5. _initialize_storage()
   - CLI flag "s3" wins over autodetection
   - reads AWS_S3_BUCKET_NAME and AWS_S3_PREFIX from settings.py
   - builds S3Storage(bucket=..., prefix="bronze")
                          ↓
6. ZoneProcessor(storage=<S3Storage>)
                          ↓
7. run_etl() loads zones_config.json, then for each zone:
                          ↓
8. extract_zone_data()
   - fetch_locations_bbox()          → save_locations_index()
   - fetch_sensors_by_location()     → save_sensors_by_location()
   - _filter_active_sensors()        (no API calls)
   - fetch_measurements_for_sensor_raw()  → save_measurements_raw()
                          ↓
9. Repeat for every zone
                          ↓
10. Print the run summary
```

---

## Data Organization

### Bronze layer (immutable source data)

**Local layout:**
```
bronze/
└── zone={zone_name}/
    ├── measurements/
    │   └── ingest_date={YYYY-MM-DD}/
    │       └── sensor_id={sensor_id}/
    │           ├── page-1.json
    │           └── page-N.json
    └── metadata/
        └── ingest_date={YYYY-MM-DD}/
            ├── locations_index.json
            ├── sensors_index.json
            └── sensors_by_location/
                └── location_id={location_id}.json
```

**S3 layout:**
```
s3://{bucket}/{prefix}/
└── zone={zone}/
    ├── measurements/ingest_date={YYYY-MM-DD}/sensor_id={id}/page-N.json
    └── metadata/ingest_date={YYYY-MM-DD}/sensors_by_location/location_id={id}.json
```

### Partitioning strategy

Hive-style (`key=value`), the layout Athena, Spark, Glue and Databricks read without extra configuration.

| Level | Meaning |
|---|---|
| `zone=` | Geographic area |
| `ingest_date=` | When the data was **fetched** |
| `sensor_id=` | Individual sensor |
| `location_id=` | Monitoring station |

Separating **ingest date** from **event date** (when the measurement was actually taken) is the decision that keeps backfills and reprocessing possible. Bronze partitions only by ingest date; Silver will partition by event date.

### Why a Bronze layer

- **Raw and immutable** — the exact API response is preserved
- **Schema-free** — no transformation applied, so future schema changes cost nothing
- **Replayable** — Silver and Gold can always be rebuilt from it
- **Audit trail** — full data lineage

The same measurement appearing under two different `ingest_date` partitions is **intended**, not a defect: it is the audit log working. Deduplication belongs to Silver.

### Actual file schemas

These reflect the real API responses in the committed sample data.

**`locations_index.json`** — wrapped in `results`; each station carries its own embedded `sensors` list:
```json
{ "results": [ {
    "id": 7719, "name": "Atemajac",
    "coordinates": { "latitude": 20.719444, "longitude": -103.355278 },
    "provider": { "id": 119, "name": "AirNow" },
    "sensors": [ { "id": 23291, "name": "co ppm",
        "parameter": { "id": 8, "name": "co", "units": "ppm", "displayName": "CO" } } ],
    "datetimeFirst": { "utc": "...", "local": "..." },
    "datetimeLast":  { "utc": "...", "local": "..." }
} ] }
```

**`sensors_by_location/location_id={id}.json`** — wrapped in `results`. Note there is no `location_id` field inside the file; the id lives in the filename:
```json
{ "results": [ {
    "id": 23291, "name": "co ppm",
    "parameter": { "id": 8, "name": "co", "units": "ppm", "displayName": "CO" },
    "datetimeFirst": { "utc": "...", "local": "..." },
    "datetimeLast":  { "utc": "...", "local": "..." },
    "coverage": { }, "latest": { }, "summary": { }
} ] }
```

**`sensors_index.json`** — a **top-level array**, not wrapped in `results`. This is a consolidated index built by the pipeline, not a raw API response:
```json
[ {
    "locationId": 7719, "locationName": "Atemajac",
    "sensorId": 23291, "parameter": "co", "units": "ppm",
    "datetimeFirst": { "utc": "...", "local": "..." },
    "datetimeLast":  { "utc": "...", "local": "..." }
} ]
```

**`measurements/.../page-N.json`**:
```json
{
  "meta": { "name": "openaq-api", "page": 1, "limit": 1000, "found": 502 },
  "results": [ {
      "period": { "label": "raw", "interval": "01:00:00",
                  "datetimeFrom": { "utc": "...", "local": "..." },
                  "datetimeTo":   { "utc": "...", "local": "..." } },
      "value": 0.0307995,
      "parameter": { "id": 8, "name": "co", "units": "ppm", "displayName": "CO" },
      "coordinates": { "latitude": 20.719444, "longitude": -103.355278 },
      "coverage": { }, "summary": { }, "flagInfo": { }
  } ]
}
```

Every measurement carries UTC time, local time, coordinates and full parameter detail, so it can be flattened to one row per reading without any joins.

**One detail that matters for Silver:** entries in `results[]` carry **no measurement `id`**. The natural deduplication key has to be built from `sensor_id` + `period.datetimeFrom.utc`.

---

## Sample Data Included

This repository ships a real Bronze dataset so the structure can be inspected without an API key, and so downstream layers have fixtures to be tested against.

| Attribute | Value |
|---|---|
| Zone | `Guadalajara_Metropolitan` |
| Measured period | from 2025-09-20, roughly 20 days |
| `ingest_date` | 2025-11-22 |
| Monitoring stations | 23 |
| Sensors in the catalog | 147 |
| Sensors with stored measurements | 77 (those that passed active-sensor filtering) |
| Parameters | 8 |

Parameters: CO (ppm), NO (ppm), NO₂ (ppm), NOx (ppm), O₃ (ppm), PM10 (µg/m³), PM2.5 (µg/m³), SO₂ (ppm).

Note the units are **not homogeneous**: gases are reported in ppm and particulates in µg/m³. Whether Silver normalizes them or preserves them with a `units` column is still an open decision — conversion depends on molecular weight plus reference temperature and pressure, so it is not a single constant.

```
bronze/zone=Guadalajara_Metropolitan/
├── measurements/ingest_date=2025-11-22/
│   ├── sensor_id=22933/page-1.json
│   ├── sensor_id=23112/page-1.json
│   └── ...
└── metadata/ingest_date=2025-11-22/
    ├── locations_index.json           # 23 stations
    ├── sensors_index.json             # 147 sensors
    └── sensors_by_location/
        ├── location_id=7719.json
        └── ...
```

Committing data to a repository is not standard production practice and it does grow the repo. It is deliberate here: this dataset is the fixture library for the tests planned in the Roadmap, and it lets anyone reading the code see real API responses immediately.

---

## Rate Limiting and Performance

The OpenAQ API allows **60 requests/minute and 2,000 requests/hour**, both tied to the personal API key.

Those two limits do not bind in the same situations. Sixty per minute sustained would be 3,600 per hour, which would blow the hourly ceiling around minute 34. A daily incremental run is limited by the per-minute cap; a historical backfill is limited by the hourly one.

`fetchers/http_client.py` implements throttling **reactively**, from the response headers:

```python
def sleep_by_rate(api_response):
    remaining = int(api_response.headers.get("x-ratelimit-remaining", "60") or 60)
    reset     = int(api_response.headers.get("x-ratelimit-reset", "1") or 1)
    if remaining <= 0:
        time.sleep(max(reset, 1))
    elif remaining <= 5:
        time.sleep(2)
    else:
        time.sleep(1.2)   # ~50 req/min, safely under 60
```

`get()` retries up to 5 times and honors `x-ratelimit-reset` when it receives an HTTP 429.

**The pipeline is sequential, and that is a decision rather than an omission.** The API is the bottleneck, not the code: the call budget is fixed, so spreading the same requests across more workers does not move the ceiling. Adding parallelism would yield roughly a 20–25% improvement, not a linear speedup, and the real benefit would be failure isolation rather than raw speed.

The current mechanism is per-process and embedded in each `get()` call, with no coordination between threads. Introducing a thread pool would therefore require replacing it with a shared, proactive limiter first.

---

## Installation and Setup

### Prerequisites

- Python 3.11 or newer
- An OpenAQ API key ([register here](https://openaq.org))
- Optionally, an AWS account with S3 access

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/MemoOrtiz/Air-Quality-ETL.git
   cd Air-Quality-ETL
   ```

2. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   The runtime dependencies are `requests`, `python-dotenv` and `boto3`.

4. **Create a `.env` file in the project root**

   For local storage:
   ```env
   OPENAQ_API_KEY=your_openaq_api_key_here
   API_BASE=https://api.openaq.org/v3
   OUT_DIR=./bronze
   ```

   For S3 storage:
   ```env
   OPENAQ_API_KEY=your_openaq_api_key_here
   API_BASE=https://api.openaq.org/v3

   AWS_S3_BUCKET_NAME=your-bucket-name
   AWS_S3_PREFIX=bronze
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_DEFAULT_REGION=us-east-1
   ```

   `OPENAQ_API_KEY`, `API_BASE` and `OUT_DIR` are read eagerly and raise a descriptive error if missing.

---

## Usage

### Auto-detected storage

Storage defaults to S3 when `AWS_S3_BUCKET_NAME` is present in `.env`, and to local otherwise.

```bash
python -m src.main \
  --zone Monterrey_Metropolitan \
  --from 2025-09-01T00:00:00Z \
  --to 2025-09-02T00:00:00Z
```

### Explicit storage selection

```bash
# Force local storage even when S3 is configured
python -m src.main --storage local \
  --zone Guadalajara_Metropolitan \
  --from 2025-10-01T00:00:00Z --to 2025-10-02T00:00:00Z

# Force S3
python -m src.main --storage s3 \
  --zone CDMX_Metropolitan \
  --from 2025-11-01T00:00:00Z --to 2025-11-02T00:00:00Z
```

### All zones at once

```bash
python -m src.main \
  --from 2025-09-01T00:00:00Z --to 2025-09-02T00:00:00Z
```

### Custom zone definitions or output directory

```bash
python -m src.main --zones ./custom_zones.json \
  --from 2025-09-01T00:00:00Z --to 2025-09-02T00:00:00Z

python -m src.main --storage local --out ./my_data \
  --from 2025-09-01T00:00:00Z --to 2025-09-02T00:00:00Z
```

### A quick first run

Start with a one-hour window to confirm credentials and layout before pulling a full range:

```bash
python -m src.main --storage local \
  --zone Monterrey_Metropolitan \
  --from 2025-11-14T00:00:00Z --to 2025-11-14T01:00:00Z
```

### Command-line options

| Option | Description | Required | Default |
|---|---|---|---|
| `--from` | Start date/time, ISO 8601 | Yes | — |
| `--to` | End date/time, ISO 8601 | Yes | — |
| `--storage` | Backend: `local` or `s3` | No | Auto-detected from `.env` |
| `--zone` | Process a single zone by name | No | All zones |
| `--zones` | Path to a zones configuration file | No | `src/ingestion/openaq/configs/zones_config.json` |
| `--out` | Base output directory, local storage only | No | `OUT_DIR` from `.env` |

---

## Configuration Reference

### Environment variables

| Variable | Description | Required |
|---|---|---|
| `OPENAQ_API_KEY` | OpenAQ API v3 authentication key | Always |
| `API_BASE` | OpenAQ API base URL | Always |
| `OUT_DIR` | Local storage base directory | Local only |
| `AWS_S3_BUCKET_NAME` | Data lake bucket name | S3 only |
| `AWS_S3_PREFIX` | Key prefix for the medallion layer, defaults to `bronze` | S3 only |
| `AWS_ACCESS_KEY_ID` | IAM credentials, read by boto3 | S3 only |
| `AWS_SECRET_ACCESS_KEY` | IAM secret, read by boto3 | S3 only |
| `AWS_DEFAULT_REGION` | AWS region, read by boto3 | S3 only |

The AWS credential variables are consumed by `boto3` directly, not by `settings.py`.

**On credentials:** static access keys in a `.env` file are convenient for local development but are not the right answer on EC2 or ECS, where an IAM role (instance profile) gives temporary, automatically rotated credentials and keeps no secrets on disk. This is acknowledged technical debt, not a recommendation.

### Adding a zone

Append an entry to `zones_config.json` with a name and a bounding box. No code changes are required:

```json
{ "name": "Puebla_Metropolitan", "bbox": [-98.35, 18.95, -98.10, 19.15] }
```

---

## Known Limitations and Design Decisions

Stated openly, because each one is a trade-off rather than an oversight.

| Item | Detail |
|---|---|
| **The storage contract is narrower than it looks** | Only `save_json` and `save_measurements_raw` are abstract. `ZoneProcessor` also calls `save_locations_index`, `save_sensors_by_location` and `save_sensors_index`, which exist only on the concrete classes. A new backend can satisfy the ABC and still break at runtime. The planned fix is to trim the contract to domain-agnostic primitives. |
| **No read methods** | Every method on the interface writes. This is precisely what blocks Silver, and adding `read_bytes`/`list_paths`/`exists` is the first item on the roadmap. |
| **Metadata overwrite semantics diverge** | `LocalStorage` skips a metadata file if it already exists; `S3Storage` always overwrites. The same logical operation behaves differently per backend, which breaks idempotency. The fix is to make both always overwrite — immutability is guaranteed by `ingest_date=` partitioning, not by refusing to rewrite a file. |
| **Latent transformation code inside ingestion** | `LocalStorage` and `ZoneProcessor` contain disabled `event_date` grouping logic that writes JSONL. It is commented out, but it lives in the wrong layer; it will be removed and rebuilt inside Silver. |
| **Small files** | One JSON per API page produces roughly 4,500 files per city per month. This hurts Spark and Athena, which spend more time opening files than reading them. Defensible in Bronze for immutability and replay, but worth naming. |
| **Sequential execution** | A deliberate choice that respects the API rate limit. See [Rate Limiting](#rate-limiting-and-performance). |
| **No structured logging** | There is no `logging` in `src/`: 82 `print()` calls. Worse, `ZoneProcessor._process_sensors` swallows per-location errors without counting them, so a run that lost three locations still reports success. Tracked in [#17](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/17). |

---

## Further Documentation

The design rationale behind this pipeline — the decoupling rules, the deployment architecture, the rate-limiting analysis, the idempotency model, and the reasoning behind each architectural decision — lives in [`docs/AirQuality_ETL_Master_Document.md`](docs/AirQuality_ETL_Master_Document.md).

That document is the project's source of truth. This README describes what the code does today; the master document explains why it is shaped that way and where it is going.

---

## Contributing

Contributions are welcome. The most useful areas right now:

- **Silver and Gold layers** — transformations and aggregations
- **Storage read methods** — the change that unblocks everything downstream
- **Additional backends** — Azure Blob Storage, Google Cloud Storage, MinIO
- **Tests** — starting with path generation and Bronze layout
- **Containerization** — per-stage Dockerfiles and split requirements

Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

---

## License

Licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.

---

## Contact

Questions, suggestions or bug reports: please open an issue on GitHub.
