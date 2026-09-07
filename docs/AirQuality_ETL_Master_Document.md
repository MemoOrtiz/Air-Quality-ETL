# Air Quality ETL — Master Document v4

> **Purpose.** Consolidate everything that exists about this project: actual state of the code, technical decisions since October 2025, deployment architecture, and execution plan. It serves as context for resuming work in any session without re-explaining anything.
>
> **Repo:** https://github.com/MemoOrtiz/Air-Quality-ETL
> **Author:** Guillermo Ortiz — Monterrey, N.L., Mexico
> **Last repo verification:** July 30, 2026 (**direct reading of the source code on the filesystem**, not inferred from the README)
> **Document version:** v4 — Jul 30, 2026

### Changes in v4

> This is the first version verified by reading the **actual source code**, file by file, rather than the README via the web. The corrections marked *Corrected* are already applied in the text of each section, as if they had always been right. Anything that turned out to be a **design decision** (and not a fact) was moved to the new §17.

| Change | Detail |
|---|---|
| Corrected | §3.2 / §5.1 — The real `StorageInterface` contract: only `save_json` and `save_measurements_raw` are `@abstractmethod` |
| Corrected | §4.5 — Real JSON schema: `{"results": […]}` wrapper on locations and sensors; `measurements` includes `coverage`, `summary`, `flagInfo` |
| Corrected | §2.2 / §3.1 — `src/transformation/` and `src/aggregation/` **do not exist** (they are not empty directories) |
| Corrected | §9.1 — Real overwrite semantics, which differ by backend |
| New | §3.7 — Code details v3 did not cover (`_filter_active_sensors`, latent `event_date` logic, HTTP retries) |
| New | §8.0 — Rate limiting **that already exists** in `http_client` (header-based + 429 retry) |
| New | §17 — Pending decisions (v4): D1–D4, arising from auditing the real code |
| Resolved | §17 — **D1–D4 decided + D5 added (2026-08-03)**; adjustments in §3.2, §5.5 (D1), §9.1 (D3), §8.0/§8.3 (D4), §9.3–§9.4 (D5) |
| Implemented | **2026-09-06** — first changes to `src/` since November 2025: the `AWS_S3_BUCKET_NAME` message (#15) and **D3** (#13, §9.1 and §17·D3). **D1, D2, D4 and D5 remain unimplemented.** The test suite is born with D3 (§10) |
| Note | §16 — v3 could not read the code; v4 could. The warning was updated |

### Changes in v3

| Change | Detail |
|---|---|
| New | §7.2 What a runner is — GitHub Actions execution model |
| New | §7.3 Parallelism calculation for the daily run (verdict: not worth it) |
| New | §7.4 Build-once-push pattern with `ghcr.io` |
| New | §7.8 **Containerized Airflow** — confirmed learning goal |
| New | Phase 7 of the plan: migration to Airflow |

### Changes in v2

| Change | Detail |
|---|---|
| Removed | `FakeStorage`. `LocalStorage` with a temporary directory already fills that role |
| Reinstated | Per-stage tests — author's decision, they will be done |
| New | §5 Decoupling — the root cause of the 8-month block |
| New | §6 Deployment architecture and per-stage Docker |
| New | §7 Orchestration |
| New | §8 Rate limiting and parallelism |
| New | §9 Idempotency and duplicates |
| New | §13 Strategic decisions (Claude Code, IaC, Azure, Glue) |
| Corrected | Rate limits confirmed by the author: 60/min **and** 2000/hour |

---

## Index

1. [Project fact sheet](#1-project-fact-sheet)
2. [Actual repository state](#2-actual-repository-state)
3. [Implemented architecture](#3-implemented-architecture)
4. [Data model and partitioning](#4-data-model-and-partitioning)
5. [Decoupling — the core problem](#5-decoupling--the-core-problem)
6. [Deployment architecture](#6-deployment-architecture)
7. [Orchestration — Actions and Airflow](#7-orchestration)
8. [Rate limiting and parallelism](#8-rate-limiting-and-parallelism)
9. [Idempotency and duplicates](#9-idempotency-and-duplicates)
10. [Per-stage tests](#10-per-stage-tests)
11. [Pending decisions](#11-pending-decisions)
12. [Decision history](#12-decision-history-oct-2025--jul-2026)
13. [Strategic decisions](#13-strategic-decisions-jul-2026)
14. [Execution plan](#14-execution-plan)
15. [Confirmed checklist](#15-confirmed-checklist)
16. [References](#16-references)
17. [Pending decisions (Jul 2026 — v4)](#17-pending-decisions-jul-2026--v4)

---

## 1. Project fact sheet

| Field | Value |
|---|---|
| **Public name** | AirQ Stream |
| **Repo** | `MemoOrtiz/Air-Quality-ETL` (public) |
| **Language** | Python 100% · 3.11+ |
| **License** | MIT |
| **Release** | v1.0.0 — *Initial Stable Release* (Nov 22, 2025) |
| **Commits** | 78 (unchanged since Nov 2025) |
| **Topics** | `aws-s3`, `air-quality`, `data-engineering`, `data-extraction`, `datalake`, `etl-pipeline`, `openaq-data`, `medallion-architecture` |
| **Source** | OpenAQ API v3 — `https://api.openaq.org/v3` |
| **Rate limits** | **60 req/min and 2,000 req/hour**, tied to the personal API key |

---

## 2. Actual repository state

### 2.1 Root

```
Air-Quality-ETL/
├── bronze/zone=Guadalajara_Metropolitan/   # real sample data
├── docs/                                    # documentation (includes this document)
├── src/
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
```

### 2.2 Status board

| Component | Status |
|---|---|
| **Bronze** (ingestion) | DONE — Functional, well architected |
| **Silver** (transformation) | NOT STARTED — `src/transformation/` **does not exist yet**. Note: there is `event_date` transformation logic **written but disabled** inside `ingestion/` (see §3.7). **D2 (§17) resolved to remove it** from `ingestion/` and keep its reference in git (commit `1062792`) as a Silver seed — pending application in code |
| **Gold** (modeling) | NOT STARTED — `src/aggregation/` **does not exist yet** |
| Tests | NOT STARTED — The README documents a `tests/` that does not exist |
| Docker | NOT STARTED — No Dockerfiles |
| CI/CD | NOT STARTED — No `.github/workflows/` |
| Orchestration | NOT STARTED — Manual CLI execution |
| Documentation | DONE — Extensive, detailed README |

### 2.3 Activity

```
Nov 22, 2025 ──────── 8 months, 0 commits ──────── Jul 30, 2026
   v1.0.0                                            (today)
   78 commits                                     78 commits
```

**Root cause identified (Jul 21, 2026):** it was not a lack of discipline. It was an unresolved design problem — how to decouple the three medallion stages, and how to read from Bronze when `StorageInterface` exposes no read methods. See §5. Stopping was the right call.

---

## 3. Implemented architecture

### 3.1 Code structure

```
src/
├── main.py                              # Bronze entry point
├── ingestion/openaq/
│   ├── cli/
│   │   ├── argument_parser.py
│   │   └── output_formatter.py
│   ├── configs/
│   │   ├── settings.py                  # reads .env (python-dotenv)
│   │   └── zones_config.json            # zones + bounding boxes
│   ├── fetchers/
│   │   ├── http_client.py               # HTTP client + rate limiting (headers + retry)
│   │   └── fetchers.py                  # fetch_locations_bbox,
│   │                                    # fetch_sensors_by_location,
│   │                                    # fetch_measurements_for_sensor_raw
│   ├── storage/
│   │   ├── storage_interface.py         # ABC — the contract (2 abstract methods)
│   │   ├── local_filesystem.py          # LocalStorage
│   │   └── s3_storage.py                # S3Storage (boto3)
│   ├── pipeline/
│   │   ├── orchestrator.py              # DataIngestionOrchestrator
│   │   └── zone_processor.py            # ZoneProcessor
│   └── utils/
│       ├── config_loader.py
│       └── helpers.py

# WARNING: src/transformation/ (Silver) and src/aggregation/ (Gold) DO NOT EXIST YET.
#          They are not empty folders: they have not been created.
```

### 3.2 The `StorageInterface` pattern

The most valuable piece of the project — and the one previous documentation most misread.

The real contract (the `@abstractmethod` methods) has **only two** methods:

```python
from abc import ABC, abstractmethod

class StorageInterface(ABC):
    @abstractmethod
    def save_json(self, path: str, data: dict): ...

    @abstractmethod
    def save_measurements_raw(self, zone, sensor_id, pages_data, ingest_date): ...
```

> **Warning: the contract is smaller than it looks.** `save_locations_index`, `save_sensors_by_location`, `save_sensors_index` and the path helpers (`zone_dir`, `metadata_dir`, `measurements_dir`) exist **only in the concrete classes** `LocalStorage`/`S3Storage`, not in the ABC. `ZoneProcessor` calls several of those methods that are **not in the contract** — today it works by *duck-typing*, but the interface does not guarantee a new backend will have them. What to do about this gap was decision **D1** (§17).

> **Resolved — D1 (§17): cut down to domain-agnostic primitives** (the ABC is not widened; it is trimmed). The **target** contract contains **only primitives**: `save_bytes`, `read_bytes`, `list_paths`, `exists` (+ `save_json` as a helper over `save_bytes`). The methods that speak OpenAQ — `save_locations_index`, `save_sensors_by_location`, `save_sensors_index` and, by the same rule, `save_measurements_raw` (today an `@abstractmethod`, but it knows about `sensor_id`/`pages_data`/`ingest_date`) — **leave the backend** and move up into the ingestion layer, built on top of the primitives with paths from `src/common/paths.py`. That way a storage adapter no longer knows what a "location" or a "sensor" is, and the same backend serves OpenAQ, Silver (Parquet), and a future source. Full rationale in **§5.5** and **§17·D1**.

| | `LocalStorage` | `S3Storage` |
|---|---|---|
| Constructor | `base="./bronze"` | `bucket_name`, `prefix="bronze"` |
| Writing | `os.path` + `open()` | `boto3.put_object()` |
| Contract methods | `save_json`, `save_measurements_raw` | `save_json`, `save_measurements_raw` |
| Extra methods (outside the contract) | `save_locations_index`, `save_sensors_by_location`, `save_sensors_index`, path helpers, **+ latent `event_date` methods** (see §3.7 and D2) | `save_locations_index`, `save_sensors_by_location`, `save_sensors_index`, path helpers |

> **Note: this table describes TODAY's state.** With D1 applied in the code, the contract becomes the **4 primitives** (`save_bytes`/`read_bytes`/`list_paths`/`exists`) + the `save_json` helper; all the semantic `save_*` methods — including `save_measurements_raw` — relocate to the ingestion layer on top of `paths.py`, and the "Extra methods" column stops making sense (they would no longer be "extra to the contract" but "owned by the stage"). The latent `event_date` methods are removed (D2).

`ZoneProcessor` receives the **interface**, not the concrete class:

```python
class ZoneProcessor:
    def __init__(self, storage: StorageInterface):
        self.storage = storage
```

**Why it matters:** real polymorphism, extensible to Azure Blob or GCS with a new class, and local development without AWS credentials. (The polymorphism is real for the two contract methods; for the rest it depends on each backend implementing them by convention — see D1.)

**Origin:** in November 2025 Repository, Hexagonal (Ports & Adapters) and Clean Architecture were studied. `StorageInterface` is Ports & Adapters — storage is a secondary/*driven* adapter.

### 3.3 Configuration chain

```
zones_config.json  ──▶ config_loader.py ──▶ orchestrator.py
.env               ──▶ settings.py      ──▶ orchestrator.py ──▶ StorageInterface
CLI flags          ──▶ argument_parser  ──▶ orchestrator.py (override)
```

```python
def storage_mode():
    """'s3' if AWS_S3_BUCKET_NAME exists, otherwise 'local'"""
    return "s3" if s3_bucket() else "local"
```

Precedence: **`--storage` flag > autodetection via `.env`**.

> ~~Note on the code: the error message in `orchestrator._initialize_storage()` mentions `S3_BUCKET_NAME`, but the variable actually read is `AWS_S3_BUCKET_NAME` (via `settings.s3_bucket()`).~~ **Fixed on 2026-09-06** (#15, commit `0c00e34`), in both `orchestrator.py` and the `argument_parser.py` help text.

### 3.4 Zones

| Zone | Bounding box `[W, S, E, N]` |
|---|---|
| `Monterrey_Metropolitan` | `[-100.60, 25.50, -99.95, 25.85]` |
| `Guadalajara_Metropolitan` | `[-103.50, 20.50, -103.20, 20.80]` |
| `CDMX_Metropolitan` | `[-99.35, 19.15, -98.95, 19.65]` |

### 3.5 Current CLI (Bronze)

| Flag | Description | Req. | Default |
|---|---|---|---|
| `--from` | Start (ISO 8601) | Yes | — |
| `--to` | End (ISO 8601) | Yes | — |
| `--storage` | `local` \| `s3` | No | Autodetection |
| `--zone` | Specific zone | No | All |
| `--zones` | Custom config | No | `configs/zones_config.json` |
| `--out` | Local base directory | No | `OUT_DIR` from `.env` |

### 3.6 Environment variables

| Variable | Use | Required |
|---|---|---|
| `OPENAQ_API_KEY` | OpenAQ authentication | Always |
| `API_BASE` | `https://api.openaq.org/v3` | Always |
| `OUT_DIR` | Local base | Local only |
| `AWS_S3_BUCKET_NAME` | Lake bucket | S3 only |
| `AWS_S3_PREFIX` | Layer prefix (default `bronze`) | S3 only |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | IAM credentials (read by boto3, not `settings.py`) | S3 only |
| `AWS_DEFAULT_REGION` | Region (read by boto3) | S3 only |

> **Warning — security debt.** Static access keys in `.env`. For EC2/ECS the correct approach is an **IAM Role** (instance profile): temporary, rotated credentials, zero secrets on disk. Already studied (Jul 2026), not applied.

### 3.7 Code details previous documentation did not cover

Things that exist in the real code and that earlier versions (inferred from the README) did not mention:

- **`ZoneProcessor._filter_active_sensors()`** — before requesting measurements, it filters out sensors whose activity period (`datetimeFirst`/`datetimeLast`) does **not** overlap the requested range. This is what reduces the 147 catalog sensors to the **77 active** ones actually queried (§4.6), and it **affects the call arithmetic** in §7.3 and §8: the real number of sensors queried per zone comes pre-filtered, it is not the catalog total.
- **Rate limiting already implemented** in `http_client` — see §8.0. `get()` retries up to 5 times and honors `429`; `sleep_by_rate()` reads the rate limit headers and adjusts the sleep.
- **Latent `event_date` transformation logic** — `LocalStorage.measurements_event_date_dir()`, `LocalStorage.save_measurements_by_event_date()` (writes **JSONL** grouped by event date) and `ZoneProcessor._organize_by_event_date()`. Today it is **disabled**: in `_process_measurements` those lines are commented out and the active flow writes only raw Bronze (`save_measurements_raw`). It is Silver-style transformation already written, living inside the ingestion layer. **D2 (§17) resolved to remove it** from `ingestion/` (Bronze stays 100% pure, R1 is satisfied) and record its location in git to recover it as a seed when building Silver. **Reminder:** Bronze stores **raw JSON exactly as it arrives from the API, not JSONL**; that JSONL grouped by `event_date` is transformed output that belongs to Silver by definition — Bronze partitions only by `zone/ingest_date/sensor_id`.

---

## 4. Data model and partitioning

### 4.1 Bronze (local)

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

### 4.2 Bronze (S3)

```
s3://{bucket}/{prefix}/
└── zone={zone}/
    ├── measurements/ingest_date={YYYY-MM-DD}/sensor_id={id}/page-N.json
    └── metadata/ingest_date={YYYY-MM-DD}/sensors_by_location/location_id={id}.json
```

### 4.3 Partitioning

Hive-style (`key=value`) — the standard that Athena, Spark, Glue and Databricks read with no extra configuration.

| Level | Meaning |
|---|---|
| `zone=` | Geographic area |
| `ingest_date=` | When the data was **fetched** |
| `event_date=` | When it was **measured** (Silver/Gold) |
| `sensor_id=` | Individual sensor |
| `location_id=` | Monitoring station |

**Key decision:** separating `ingest_date` from `event_date`. Uncommon in junior profiles and it is what enables backfills and reprocessing.

### 4.4 Format and location per layer

| Layer | Format | Partition | Location | Status |
|---|---|---|---|---|
| Bronze | Raw JSON, immutable | `zone=/ingest_date=/sensor_id=` | `bronze/` | DONE |
| Silver | Parquet (snappy) | `zone=/event_date=` | `silver/` | NOT STARTED |
| Gold | Parquet | to be defined | `gold/` | NOT STARTED |

**Confirmed:** same bucket in S3 (or same root folder locally), separated by the `bronze/`, `silver/`, `gold/` prefixes.

### 4.5 Real file schema (verified against the sample data)

> **Corrected in v4.** The top-level wrapper of the metadata is **`results`**, not `locations` or `sensors`, and the `sensors_by_location` files do **not** contain a `location_id` field (the id is in the file name).

**`locations_index.json`** — wrapped in `results`; each station already carries its embedded `sensors` list:
```json
{ "results": [ {
    "id": 7719, "name": "Atemajac",
    "coordinates": { "latitude": 20.719444, "longitude": -103.355278 },
    "provider": { "id": 119, "name": "AirNow" },
    "sensors": [ { "id": 23291, "name": "co ppm",
        "parameter": { "id": 8, "name": "co", "units": "ppm", "displayName": "CO" } } ],
    "datetimeFirst": { "utc": "...", "local": "..." },
    "datetimeLast":  { "utc": "...", "local": "..." }
    // + timezone, country, owner, isMobile, isMonitor, instruments, licenses, bounds
} ] }
```

**`sensors_by_location/location_id={id}.json`** — wrapped in `results`; no `location_id` field:
```json
{ "results": [ {
    "id": 23291, "name": "co ppm",
    "parameter": { "id": 8, "name": "co", "units": "ppm", "displayName": "CO" },
    "datetimeFirst": { "utc": "...", "local": "..." },
    "datetimeLast":  { "utc": "...", "local": "..." },
    "coverage": { "...": "..." }, "latest": { "...": "..." }, "summary": { "...": "..." }
} ] }
```

**`sensors_index.json`** — **top-level array** (NOT wrapped in `results`), a consolidated index owned by the pipeline:
```json
[ {
    "locationId": 7719, "locationName": "Atemajac", "city": "Unknown",
    "provider": "Unknown", "sensorId": 23291,
    "parameter": "co", "units": "ppm",
    "datetimeFirst": { "utc": "...", "local": "..." },
    "datetimeLast":  { "utc": "...", "local": "..." }
} ]
```

**`measurements/.../page-N.json`**:
```json
{
  "meta": { "name": "openaq-api", "website": "/", "page": 1, "limit": 1000, "found": 502 },
  "results": [ {
      "period": { "label": "raw", "interval": "01:00:00",
                  "datetimeFrom": { "utc": "...", "local": "..." },
                  "datetimeTo":   { "utc": "...", "local": "..." } },
      "value": 0.0307995,
      "parameter": { "id": ..., "name": "...", "units": "...", "displayName": "..." },
      "coordinates": { "latitude": ..., "longitude": ... },
      "coverage": { "...": "..." }, "summary": { "...": "..." }, "flagInfo": { "...": "..." }
  } ]
}
```

**Implication:** every measurement carries UTC, local time, coordinates and parameter detail (plus `coverage`/`summary`/`flagInfo`). It can be flattened to one row per measurement without joins. **Important note for Silver:** `results[]` does **not** include a measurement `id` field — the natural deduplication key will have to be built from `sensor_id` + `period.datetimeFrom.utc` (see §11.2).

### 4.6 Sample data in the repo

| Attribute | Value |
|---|---|
| Zone | `Guadalajara_Metropolitan` |
| Measured period | 2025-09-20 onward (~20 days) |
| `ingest_date` | 2025-11-22 |
| Stations | 23 |
| Sensors in the catalog | 147 (`sensors_index.json`) |
| Sensors with stored measurements | 77 (those that passed `_filter_active_sensors`, §3.7) |
| Parameters | 8 |

CO (ppm), NO (ppm), NO₂ (ppm), NOx (ppm), O₃ (ppm), PM10 (µg/m³), PM2.5 (µg/m³), SO₂ (ppm).

> **The units are not homogeneous** (ppm vs µg/m³). See the pending decision in §11.
>
> This data is also the **fixture library** for the tests in §10.

---

## 5. Decoupling — the core problem

### 5.1 The root cause of the block

The `StorageInterface` contract (the `@abstractmethod` methods) is:

```
save_json()   save_measurements_raw()
```

And the concrete classes add, **outside the contract**:

```
save_locations_index()   save_sensors_by_location()   save_sensors_index()
```

**They all write. None reads.** It was designed for ingestion, and ingestion only writes.

When starting Silver it became necessary to *read* the `page-N.json` files from Bronze — and the abstraction did not allow it. At that point Glue looks attractive because it reads S3 natively and sidesteps the problem, but it introduces new unknowns. The real blocker was the missing read methods (§5.5). *(The scope of the contract — which methods should be abstract — was resolved in **D1** (§17): the port is trimmed to primitives and the semantic OpenAQ methods move up to the stage. See §5.5.)*

### 5.2 The three rules

**R1 — No stage imports another.**
Silver never does `from src.ingestion... import`. If it does, they are coupled.

**R2 — The contract is the data at rest, not a function call.**

```
Bronze ──writes──▶ bronze/ ──reads──▶ Silver ──writes──▶ silver/ ──reads──▶ Gold ──▶ gold/
```

There are no arrows between the *code* of the stages. Only between code and storage.

**R3 — Each stage runs alone and is idempotent.**
Silver must be able to run over data ingested three weeks ago without touching Bronze. And running it five times gives the same result.

### 5.3 Direction of dependencies

```
              ┌──────────────────┐
              │ StorageInterface │
              └────────┬─────────┘
          ┌────────────┼────────────┐
       Bronze        Silver        Gold
```

All three depend on the interface. None depends on another. It is the same dependency inversion as `LocalStorage`/`S3Storage`, now applied to the pipeline.

### 5.4 What is independent and what is not

| Level | Independent? | Guaranteed by |
|---|---|---|
| Code | Yes | Zero cross imports |
| Dependencies | Yes | `requirements` split per stage |
| Process / runtime | Yes | Different image and container |
| Execution and schedule | Yes | Own trigger per stage |
| Failure | Yes | If Silver breaks, Bronze's output is still there |
| **Data** | **No — and that is how it should be** | The storage layout is the contract |

Silver *depends on data existing* in `bronze/`, but **not on Bronze's code**. It can run over data from three weeks ago or placed by hand. That is the real independence.

> **Warning — v4 caveat.** Today R1 is not fully satisfied: there is `event_date` transformation logic inside `ingestion/` (§3.7). It is disabled, but it lives in the wrong layer. **D2 (§17) resolved to remove it** (keeping its git reference as a Silver seed); once applied in the code, R1 holds. Until `src/` is touched, the caveat stands.

### 5.5 The unblock (~40 lines)

Add to `storage_interface.py`:

```python
@abstractmethod
def read_bytes(self, path: str) -> bytes: ...

@abstractmethod
def save_bytes(self, path: str, data: bytes): ...

@abstractmethod
def list_paths(self, prefix: str) -> list[str]: ...

@abstractmethod
def exists(self, path: str) -> bool: ...
```

Implementations:

| Method | `LocalStorage` | `S3Storage` |
|---|---|---|
| `read_bytes` | `open(path,'rb').read()` | `get_object()['Body'].read()` |
| `save_bytes` | `open(path,'wb').write()` | `put_object(Body=...)` |
| `list_paths` | `os.walk` / `glob` | `list_objects_v2` with paginator |
| `exists` | `os.path.exists` | `head_object` |

**Design detail:** use `read_bytes`/`save_bytes` as primitives, **not** `read_parquet`. The interface must not know what a DataFrame is. Silver serializes to Parquet in memory and passes bytes. That way storage stays format-agnostic and Gold can use another format without touching the interface.

`save_json` becomes a helper that calls `save_bytes`.

#### The final contract and where the semantic methods live (D1 — resolved)

> These four read/write methods are **not added** to a contract that also demands `save_locations_index` and company. They are **the whole** contract. That is decision **D1** (§17).

The `StorageInterface` port ends up with **exactly** these domain-agnostic primitives:

```
save_bytes()   read_bytes()   list_paths()   exists()      # abstract
save_json()                                                 # helper over save_bytes
```

And **outside the backend**, one level up (in the ingestion layer), live the methods that speak OpenAQ, built on the primitives and using `src/common/paths.py` to compute paths:

```python
# sketch — ingestion layer, NOT inside StorageInterface
def save_locations_index(storage, zone, data, ingest_date):
    path = paths.bronze_metadata_prefix(zone, ingest_date) + "/locations_index.json"
    storage.save_json(path, data)

# save_sensors_by_location, save_sensors_index and save_measurements_raw
# follow the same pattern: domain on top, primitives underneath.
```

**`save_measurements_raw` also drops from the ABC down to the stage.** Today it is an `@abstractmethod`, but it knows `sensor_id`, `pages_data` and `ingest_date` — Bronze vocabulary, not "storage" vocabulary. For consistency with the cut, it stops being part of the port and becomes an ingestion function that serializes each page with `save_json`/`save_bytes` over paths from `paths.py`.

**Why this cut and not widening the ABC (original Option A in §17):**

| | Widen the ABC (A) | Cut to primitives (chosen) |
|---|---|---|
| Does the backend know the OpenAQ domain? | Yes — forced to implement `save_locations_index` | **No** — only bytes and paths |
| A new backend (Azure/MinIO/GCS) implements | ~7+ methods, some its consumer never uses | **4 primitives** + `save_json` helper |
| Does it serve Silver (Parquet, no "locations")? | Dead or forced method | **Yes, unchanged** — Silver uses the same primitives |
| Does it serve a future source (NASA…)? | The ABC must be widened again | **Yes** — the new domain lives in its stage |
| Risk of an "incomplete backend" at runtime | Eliminated, but at the cost of every adapter | Eliminated for what matters (I/O); the domain is guaranteed by the stage, not by the port |

The secondary (*driven*) adapter goes back to what Ports & Adapters prescribes: **narrow, stable and blind to the domain**. Knowledge of OpenAQ stays on the primary adapter side (ingestion), on top of `paths.py`.

### 5.6 Three entry points

| Stage | Entry point | CLI |
|---|---|---|
| Bronze | `src/main.py` | `--zone --from --to --storage` |
| Silver | `src/transformation/main.py` | `--zone --rebuild \| --ingest-date --storage` |
| Gold | `src/aggregation/main.py` | `--zone --date-range --storage` |

### 5.7 The shared contract

`src/common/paths.py` — Bronze, Silver and Gold **all import from there**. That does not couple them to each other: it couples them to a neutral, stable third party. It is the correct pattern.

```python
# src/common/paths.py (sketch)
def bronze_measurements_prefix(zone, ingest_date=None): ...
def bronze_metadata_prefix(zone, ingest_date=None): ...
def silver_partition(zone, event_date): ...
def gold_partition(zone, grain): ...
```

---

## 6. Deployment architecture

### 6.1 The principle

**It is not "Docker on my laptop **or** ECS". It is the same artifact running on both sides.**

```
ONE repo  ──build──▶  THREE images  ──run──▶  anywhere
                      airq-bronze              laptop (dev)
                      airq-silver              GitHub Actions (schedule)
                      airq-gold                ECS Fargate (prod)
```

This is not a "mega project". It is a **monorepo with three independent deliverables** — the standard pattern on data teams. Three separate repos would be more decoupled in theory, but for a single developer it is pure pain: versioning `paths.py` three times, three CI pipelines, syncing changes.

**Independence does not come from the number of repos. It comes from the container boundary: one image cannot import code from another.** That is R1 enforced by construction.

### 6.2 File structure

```
Air-Quality-ETL/
├── docker/
│   ├── bronze.Dockerfile
│   ├── silver.Dockerfile
│   └── gold.Dockerfile
├── requirements/
│   ├── bronze.txt        requests, boto3, python-dotenv
│   ├── silver.txt        pandas, pyarrow, boto3, python-dotenv
│   └── gold.txt          pandas, pyarrow, boto3, python-dotenv
├── .dockerignore
└── src/
    ├── common/
    ├── ingestion/
    ├── transformation/
    └── aggregation/
```

**Bronze does not need pandas.** Today there is a single `requirements.txt` for everything (`requests`, `python-dotenv`, `boto3`). Splitting it is trivial and it is the cleanest proof that the stages are independent.

### 6.3 `docker/bronze.Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app

COPY requirements/bronze.txt .
RUN pip install --no-cache-dir -r bronze.txt

# Only the code Bronze needs
COPY src/common/    ./src/common/
COPY src/ingestion/ ./src/ingestion/
COPY src/main.py    ./src/

ENTRYPOINT ["python", "-m", "src.main"]
```

### 6.4 `docker/silver.Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app

COPY requirements/silver.txt .
RUN pip install --no-cache-dir -r silver.txt

COPY src/common/         ./src/common/
COPY src/transformation/ ./src/transformation/

ENTRYPOINT ["python", "-m", "src.transformation.main"]
```

### 6.5 Why the selective `COPY`

The Bronze image **physically does not contain** Silver's code. Verifiable:

```bash
docker run --rm --entrypoint ls airq-bronze -R /app/src
# transformation/ does not appear
```

Decoupling that is verifiable, not promised.

### 6.6 Build and run

```bash
docker build -f docker/bronze.Dockerfile -t airq-bronze:latest .
docker build -f docker/silver.Dockerfile -t airq-silver:latest .

# Local — mount the volume
docker run --rm --env-file .env \
  -v "$(pwd)/bronze:/app/bronze" \
  airq-bronze --zone Monterrey_Metropolitan \
              --from 2025-09-01T00:00:00Z --to 2025-09-02T00:00:00Z

# S3 — no volume, only variables
docker run --rm --env-file .env \
  airq-bronze --storage s3 --zone CDMX_Metropolitan --from ... --to ...
```

### 6.7 Details that matter

- **`ENTRYPOINT` instead of `CMD`**: whatever is written after the image name is appended as arguments. The current CLI works unchanged.
- **Build context at the root** (the trailing `.`); that is why `-f` points at the Dockerfile.
- **`.dockerignore` is mandatory**: `bronze/`, `silver/`, `gold/`, `.venv/`, `.env`, `.git/`, `__pycache__/`. Without it, the sample JSON files get copied into the image.

---

## 7. Orchestration

### 7.1 Comparison

| Option | Failure isolation | Cost | When |
|---|---|---|---|
| **GitHub Actions matrix** | `fail-fast: false` + re-run of the failed job | Free (public repo) | **Now** |
| **local cron + Docker** | `\|\|` per zone, manual | $0 | Dev / testing |
| **Containerized Airflow** | DAG with per-task retries, native backfill | $0 on Docker (MWAA ~$300/month — rejected) | **Confirmed learning goal** |
| **AWS Step Functions** | Map state with native retry/catch per branch | ~$0.03/month at this scale | AWS-native |
| **EventBridge + ECS Fargate** | Task per zone, retries | Cents | Production on AWS |
| **Dagster self-hosted** | Assets with lineage, retry per partition | $0 on Docker | Alternative to Airflow |
| **Azure Data Factory** | Activities with retry | Azure credit | With the new account |

**Decision: GitHub Actions now** (zero infrastructure, unblocks today) → **containerized Airflow later** (§7.8), once Gold exists and there is a real dependency graph to express.

---

### 7.2 What a runner is (the Actions execution model)

A GitHub Actions **job** = a **fresh, clean VM** that GitHub lends you, runs the steps, and destroys when it finishes.

A `matrix` with 3 zones does **not** create 3 containers on one VM. It creates **3 jobs = 3 VMs**, each with its own disk, its own RAM and its own Docker daemon.

```
Job: Monterrey          Job: Guadalajara       Job: CDMX
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ ephemeral VM     │   │ ephemeral VM     │   │ ephemeral VM     │
│  git clone       │   │  git clone       │   │  git clone       │
│  docker build    │   │  docker build    │   │  docker build    │
│  docker run      │   │  docker run      │   │  docker run      │
│   └─ RateLimiter │   │   └─ RateLimiter │   │   └─ RateLimiter │
│ (destroyed)      │   │ (destroyed)      │   │ (destroyed)      │
└──────────────────┘   └──────────────────┘   └──────────────────┘
```

**The Docker image is the recipe; each VM is a different kitchen.** Same recipe, three kitchens that do not talk to each other.

#### The two levels of parallelism

**Within a zone — `ThreadPoolExecutor`** (shared memory — works)

```
┌─────────── ONE Python process ───────────┐
│   thread 1 ─┐                             │
│   thread 2 ─┼──▶ RateLimiter  ← ONE object│
│   thread 3 ─┤     (deque + lock)  in RAM  │
│   thread 4 ─┘                             │
└───────────────────────────────────────────┘
                    ↓  at most 55 req/min — correct
```

**Across zones — Actions matrix** (isolated memory — broken)

```
┌── Runner A ──┐  ┌── Runner B ──┐  ┌── Runner C ──┐
│ RateLimiter  │  │ RateLimiter  │  │ RateLimiter  │
│   ↓ 55/min   │  │   ↓ 55/min   │  │   ↓ 55/min   │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       └─────────────────┴─────────────────┘
                         ↓
             SAME API key → 165 req/min — broken
```

OpenAQ's limit is **per API key**, not per runner. Three `RateLimiter` objects in different processes cannot coordinate: their state lives in local RAM.

> **Analogy.** A club with a capacity of 60. One door with one bouncer counting → fine. Three doors with three bouncers who do not talk to each other, each letting 55 through → 165 inside.

#### `matrix` ≠ parallelism

| Property | Where it comes from |
|---|---|
| **Failure isolation** | From having separate jobs — it exists even with `max-parallel: 1` |
| **Parallelism** | From `max-parallel: N` |

They are independent. `matrix` means **one job per element**; `max-parallel` controls how many run at once. With `max-parallel: 1` the zones run in a queue, but if CDMX breaks, Monterrey and Guadalajara have already finished and been saved — and you re-run only CDMX from the UI.

#### The hidden cost

Each job pays VM startup + `git clone` + `docker build` ≈ **1–2 min**. With 8 zones that is **8–16 minutes of pure overhead** against ~11 min of real work. More than half the time goes into setting up and tearing down kitchens. See the solution in §7.4.

---

### 7.3 Is it worth parallelizing the daily run?

**Verdict: no.** And for the same reason as in the historical case.

Daily, with the catalog already split out to weekly: ~77 active sensors per zone (after `_filter_active_sensors`, §3.7) × 1 page (24 hourly measurements fit easily within the 1000 limit) = **~77 calls per zone**. Eight zones ≈ **616 calls**.

| Strategy | API time |
|---|---|
| Sequential across zones, 55/min | 616 ÷ 55 = **~11 min** |
| 8 zones in parallel, budget split (8 × ~7/min) | 616 ÷ 55 = **~11 min** |
| 8 zones in parallel, distributed limiter on Redis | 616 ÷ 55 = **~11 min** |

**The call budget is fixed.** Parallelizing spreads the same 616 calls across more processes, but the 55/min ceiling does not move. It is like having eight tellers in a bank where only one customer per minute walks in.

The only thing parallelism can hide is **infrastructure overhead**, not API time. And for that there is a simpler solution than a distributed limiter: eliminate the overhead (§7.4).

> **Corollary:** the only way to genuinely speed this up is a higher limit negotiated with OpenAQ. Neither threading nor orchestration moves that ceiling.

---

### 7.4 Build-once-push pattern (recommended)

One job builds and publishes the image to `ghcr.io`; the matrix jobs only `pull`. Per-zone overhead drops from ~2 min to ~30 s, and selective re-run is preserved.

```yaml
name: bronze-daily
on:
  schedule:
    - cron: '0 8 * * *'     # 2am Monterrey time (UTC-6)
  workflow_dispatch:         # manual button for backfills

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: |
          echo ${{ secrets.GITHUB_TOKEN }} | \
            docker login ghcr.io -u ${{ github.actor }} --password-stdin
          docker build -f docker/bronze.Dockerfile \
            -t ghcr.io/memoortiz/airq-bronze:latest .
          docker push ghcr.io/memoortiz/airq-bronze:latest

  extract:
    needs: build               # ← dependency between jobs
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false         # ← failure isolation
      max-parallel: 1          # ← protects the rate limit
      matrix:
        zone:
          - Monterrey_Metropolitan
          - Guadalajara_Metropolitan
          - CDMX_Metropolitan
    steps:
      - run: |
          docker run --rm \
            -e OPENAQ_API_KEY=${{ secrets.OPENAQ_API_KEY }} \
            -e AWS_S3_BUCKET_NAME=${{ secrets.AWS_S3_BUCKET_NAME }} \
            -e AWS_ACCESS_KEY_ID=${{ secrets.AWS_ACCESS_KEY_ID }} \
            -e AWS_SECRET_ACCESS_KEY=${{ secrets.AWS_SECRET_ACCESS_KEY }} \
            ghcr.io/memoortiz/airq-bronze:latest \
            --storage s3 --zone ${{ matrix.zone }} --from ... --to ...
```

**Why this pattern and not a single monolithic job:** it teaches image registries (`ghcr.io`), which is what any real team does, and `needs:` introduces the concept of dependencies between tasks — exactly what is used later in Airflow. A single job would be marginally faster but teaches less and loses the visible isolation.

**The coherent design: isolation across zones; speed within the zone.**

---

### 7.5 Alternative: a single job, all zones

One VM, one build, **a single in-memory `RateLimiter`** shared by everything. It is the ideal situation for rate limiting: zero distributed coordination. Failure isolation done in code:

```python
results = {}
for zone in zones:
    try:
        results[zone] = process_zone(zone, limiter)
    except Exception as e:
        results[zone] = {"error": repr(e)}   # continue with the next one
```

Total time ~13 min (11 of API + 2 of setup). Downside: re-running one failed zone costs the whole run. It is documented as a valid option, but §7.4 is preferred.

---

### 7.6 Weekly catalog workflow

```yaml
name: catalog-weekly
on:
  schedule:
    - cron: '0 6 * * 0'    # Sundays
  workflow_dispatch:
```

Stations and sensors change every few months, not every hour. Today they are re-requested on every run: ~24 calls per zone × 8 cities ≈ **190 wasted calls**, ~10% of the hourly budget. Splitting it out recovers that without writing any concurrency.

---

### 7.7 GitHub Actions warnings

1. **Scheduled workflows are disabled after 60 days without commits** in the repo. With 8 months of inactivity, this was going to bite.
2. **Secrets in GitHub Secrets**, never in a committed `.env`.
3. **GitHub's cron is not punctual** — it can be delayed by minutes. Irrelevant for a nightly batch.

---

### 7.8 Containerized Airflow

> **Confirmed learning goal.** It will be containerized.

#### Why the migration is cheap

The decision to containerize (§6) is precisely what makes it cheap. Today:

```yaml
- run: docker run airq-bronze --zone X --from ... --to ...
```

Later:

```python
DockerOperator(
    task_id=f"bronze_{zone}",
    image="airq-bronze:latest",
    command=f"--zone {zone} --from {{{{ ds }}}} --to {{{{ next_ds }}}}",
)
```

**It is the same command.** Migrating means rewriting ~40 lines of YAML into ~40 of Python.

> **The orchestrator is a swappable adapter** — the same principle as `StorageInterface`, applied one level up.

#### The rule that protects portability

**Zero business logic in the orchestrator.** If the workflow only does `docker run`, the migration takes an afternoon. If transformations or business `if`s get pushed into the YAML (or into the DAG), you are tied to that tool.

#### When to migrate

**Not before Gold exists.** Today there is no dependency graph — there are three commands. Airflow with a scheduler, webserver, metadata database and executor to run three containers a day is over-engineering.

Migrate when the real chain exists:

```
catalog_weekly ──▶ bronze ──▶ silver ──▶ gold
```

#### What Airflow gives that Actions does not

| Capability | Detail |
|---|---|
| **Native backfill** | `airflow dags backfill -s 2025-09-01 -e 2025-10-31` generates one run per logical date. Maps directly to the `ingest_date=` partitioning and to the case of pulling months of history |
| **Logical dates** | `{{ ds }}` and `{{ next_ds }}` inject the range automatically — the manual `--from`/`--to` computation disappears |
| **Per-task retries** | With exponential backoff, configurable per task |
| **Sensors** | Wait for a file to exist before triggering the next stage |
| **Visual graph** | See the pipeline and where it failed |
| **SLAs** | Alerts if a task takes longer than expected |

#### The career argument

Different from the technical one and also legitimate: **Airflow shows up in job postings; GitHub Actions does not count as data orchestration** to a recruiter. At this scale Actions is probably enough forever — the migration is for learning and for the résumé. It is worth naming it that honestly.

#### Containerized setup

Airflow is brought up with the official `docker-compose.yaml`. Components: scheduler, webserver, metadata database (Postgres) and executor.

```
airflow/
├── docker-compose.yaml       # official Apache Airflow
├── dags/
│   └── airq_pipeline.py
├── logs/
└── plugins/
```

**Delicate point — Docker-in-Docker.** For `DockerOperator` to launch the pipeline containers, the host's Docker socket has to be mounted into the Airflow container:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

It is the standard pattern in local development. Warning: it implies elevated privileges — acceptable on the laptop, **not** in production without review.

#### DAG sketch

```python
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

ZONES = ["Monterrey_Metropolitan",
         "Guadalajara_Metropolitan",
         "CDMX_Metropolitan"]

with DAG(
    dag_id="airq_pipeline",
    start_date=datetime(2025, 9, 1),
    schedule="0 8 * * *",
    catchup=False,               # ← True enables automatic backfill
    max_active_tasks=1,          # ← equivalent to max-parallel: 1
    default_args={"retries": 2},
) as dag:

    bronze_tasks = []
    for zone in ZONES:
        bronze_tasks.append(
            DockerOperator(
                task_id=f"bronze_{zone}",
                image="airq-bronze:latest",
                command=f"--storage s3 --zone {zone} "
                        f"--from {{{{ ds }}}}T00:00:00Z "
                        f"--to {{{{ next_ds }}}}T00:00:00Z",
                docker_url="unix://var/run/docker.sock",
                auto_remove=True,
            )
        )

    silver = DockerOperator(
        task_id="silver_rebuild",
        image="airq-silver:latest",
        command="--storage s3 --rebuild",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
    )

    gold = DockerOperator(
        task_id="gold_build",
        image="airq-gold:latest",
        command="--storage s3",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
    )

    bronze_tasks >> silver >> gold
```

**Details that matter:**

- **`max_active_tasks=1`** is the equivalent of `max-parallel: 1`. The reason is identical: each `DockerOperator` is a container with its own in-memory `RateLimiter`. §7.2 applies here too.
- **`{{ ds }}` and `{{ next_ds }}`** are Airflow's logical dates. With `catchup=True`, a historical range generates one run per day automatically — this is native backfill.
- **`bronze_tasks >> silver >> gold`** expresses the dependency. Silver starts only when all Bronze tasks have finished successfully.
- **`retries: 2`** by default on every task.

#### Airflow vs Dagster

| | Airflow | Dagster |
|---|---|---|
| Market share | Dominant | Growing |
| Model | Tasks and DAGs | Assets with lineage |
| Fit with this design | Good | **Better** — its partitions map directly to `event_date` |
| Learning curve | Heavier | Cleaner |

**Decision: Airflow**, for employability. Both run the same containers, so the choice is not a lock-in: trying Dagster later costs little.

---

## 8. Rate limiting and parallelism

### 8.0 What already exists today (do not rewrite blindly)

> **v4.** Rate limiting does **not** start from zero. `fetchers/http_client.py` already implements it, in a **reactive, per-process global** way:

```python
def sleep_by_rate(api_response):
    """OpenAQ rate limit (60/min, 2000/hour)"""
    remaining = int(api_response.headers.get("x-ratelimit-remaining", "60") or 60)
    reset     = int(api_response.headers.get("x-ratelimit-reset", "1") or 1)
    if remaining <= 0:
        time.sleep(max(reset, 1))
    elif remaining <= 5:
        time.sleep(2)
    else:
        time.sleep(1.2)   # ~50 req/min (safely under 60)

def get(url, params=None, max_retries=5):
    # retries up to 5 times; on 429 it honors x-ratelimit-reset
    ...
```

- It is **reactive**: it reacts to the headers *after* receiving the response.
- It is **per-process global** and embedded in every `get()`; there is **no** shared `RateLimiter` object and no coordination between threads.
- It works for the current sequential pipeline. But as soon as `ThreadPoolExecutor` is introduced (§8.4), the `sleep` inside `get()` **does not coordinate** the budget across threads.

How this relates to the dual-window `RateLimiter` proposed below (§8.3) — replace it, combine it, or keep it — was resolved in **D4** (§17): **Option A — replace** `sleep_by_rate` with the proactive `RateLimiter` from §8.3, calling `acquire()` before each request (including pagination). It implies rewriting the fetcher signatures to receive the limiter, and it is a **prerequisite for `ThreadPoolExecutor` (§8.4)** because the current mechanism does not coordinate across threads. What follows in §8.1–§8.6 is a **proposal**, not what exists today.

### 8.1 The two limits are not the same limit

**Confirmed: 60 req/min and 2,000 req/hour, tied to the personal API key.**

60/min sustained would be 3,600/hour — it would blow the hourly limit at minute 34.

| Scenario | Approx. volume | Binding limit | Effective rate |
|---|---|---|---|
| **Daily incremental** (1 day × 8 cities) | ~620 calls, ~11 min | 60/min | ~55 req/min |
| **Historical backfill** (2 months × 8 cities) | ~1,500–4,000 calls | **2,000/hour** | ~31 req/min |

History is where it hurts, and there the ceiling is the hourly one.

**Honest consequence:** on backfill, 1,900/hour is nearly saturated with **a single thread**. Threading helps the daily run, not the historical one. History is limited by the API, not by concurrency.

### 8.2 Baseline measurement

8 cities × several days took **~40 minutes**. Estimating ~77 active sensors × ~3 pages ≈ 230 calls per city → ~1,840 calls in 40 min = **~46 req/min**, already at ~77% of the per-minute ceiling.

| Scenario | Result |
|---|---|
| Today (sequential) | ~46 req/min → 40 min |
| Parallelism with a shared limiter | ~58 req/min → **~32 min** |
| Naive parallelism without a limiter | 429s, backoff, **slower** |

**The real gain is ~20-25%, not 8×.** The bottleneck is the API, not concurrency. The big gain is in **failure isolation** (§7.2): a failure in city 6 should not take down cities 7 and 8.

### 8.3 Dual-window `RateLimiter` (proposed)

```python
import time, threading
from collections import deque

class RateLimiter:
    """Dual window: N/minute and M/hour. Shared by all workers.

    Conservative default margins (55/1900 instead of 60/2000) to leave
    room for retries and for clock skew against the server.
    """

    def __init__(self, per_minute=55, per_hour=1900):
        self.per_minute = per_minute
        self.per_hour = per_hour
        self._calls = deque()          # monotonic timestamps
        self._lock = threading.Lock()

    def acquire(self):
        """Blocks until there is room in BOTH windows. Thread-safe."""
        while True:
            with self._lock:
                now = time.monotonic()

                # purge whatever has left the hourly window
                while self._calls and now - self._calls[0] > 3600:
                    self._calls.popleft()

                in_minute = sum(1 for t in self._calls if now - t <= 60)
                in_hour = len(self._calls)

                if in_minute < self.per_minute and in_hour < self.per_hour:
                    self._calls.append(now)
                    return

                # compute how long to wait depending on which window blocks
                if in_minute >= self.per_minute:
                    oldest_in_min = next(t for t in self._calls if now - t <= 60)
                    wait = 60 - (now - oldest_in_min) + 0.05
                else:
                    wait = 3600 - (now - self._calls[0]) + 0.05

            # ← sleep OUTSIDE the lock
            time.sleep(min(wait, 5))
```

**Two details that are not details:**

- **Sleep outside the lock.** Sleeping inside blocks the other threads and the pool stops serving.
- **Margins of 55/1900**, not 60/2000. Room for retries and clock skew.

**Relationship with what exists (§8.0):** the current `sleep_by_rate` already reads `x-ratelimit-remaining`. **D4 (§17) resolved to replace it (Option A)** with this proactive `RateLimiter`. Honest note: by choosing A, the header stops being the "first line"; the `429` retry in `get()` can be kept as a safety net, but the budget governor becomes `acquire()`, not the reactive `sleep`.

### 8.4 `ThreadPoolExecutor` within the zone (proposed)

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_zone_data(self, zone, bbox, dt_from, dt_to, ingest_date, limiter):
    # 1. catalog — sequential, few calls
    locations = fetch_locations_bbox(bbox, limiter)
    self.storage.save_locations_index(zone, locations, ingest_date)

    sensors = self._collect_sensors(locations, ingest_date, limiter)

    # 2. measurements — parallel per sensor
    ok, failed = 0, []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(self._process_sensor, zone, s,
                        dt_from, dt_to, ingest_date, limiter): s
            for s in sensors
        }
        for fut in as_completed(futures):
            sensor = futures[fut]
            try:
                fut.result()
                ok += 1
            except Exception as e:
                # one failing sensor does NOT take down the zone
                failed.append((sensor["id"], repr(e)))

    return {"ok": ok, "failed": failed}


def _process_sensor(self, zone, sensor, dt_from, dt_to, ingest_date, limiter):
    pages = fetch_measurements_for_sensor_raw(
        sensor["id"], dt_from, dt_to, limiter
    )
    self.storage.save_measurements_raw(zone, sensor["id"], pages, ingest_date)
```

> Note: today the fetchers do **not** receive a `limiter`; their real signature is, e.g., `fetch_measurements_for_sensor_raw(sensor_id, dt_from, dt_to, limit=PAGE_LIMIT_DEFAULT)` and rate limiting lives inside `get()` (§8.0). This proposal changes that signature.

### 8.5 Parallelism rules

**`limiter.acquire()` goes before EVERY HTTP request** — including inside the fetcher's pagination loop, not just once per sensor. It is the easiest mistake to make.

```python
def fetch_measurements_for_sensor_raw(sensor_id, dt_from, dt_to, limiter):
    pages, page = [], 1
    while True:
        limiter.acquire()          # ← HERE, on every iteration
        resp = http_get(f"/sensors/{sensor_id}/measurements", params={...})
        data = resp.json()
        pages.append(data)
        if len(data["results"]) < data["meta"]["limit"]:
            break
        page += 1
    return pages
```

**On `max_workers=4`:** the limiter is the real governor, not the thread count. With ~1.2 s of latency, 2 threads already saturate 55/min; 4 gives slack for slow responses. Raising it to 16 speeds up nothing — it just makes 12 threads sleep.

**`ThreadPoolExecutor` and not `asyncio`:** the code uses synchronous `requests`, the pool is a drop-in, and the GIL is released while waiting on the network. `asyncio` would require rewriting every fetcher with `httpx`/`aiohttp` for the same gain.

### 8.6 Two things that break with threads

**`ensure_dir` in `LocalStorage`.** Two threads creating the same directory at once raise an exception. (The current code already uses `Path(path).mkdir(parents=True, exist_ok=True)` in `helpers.ensure_dir`, so this risk is already mitigated — confirm when parallelizing.)

```python
os.makedirs(path, exist_ok=True)    # ← equivalent to the fix; already present via Path.mkdir(exist_ok=True)
```

**`boto3`.** *Clients* are considered thread-safe; *resources* are not. `S3Storage` creates a client in `__init__`, so it should be fine — **confirm with a real run** before trusting it.

---

## 9. Idempotency and duplicates

### 9.1 The diagnosis

`ingest_date` in Bronze is **correct**. Duplicates across partitions are deliberate, not a defect.

| Case | Real behavior | Idempotent? |
|---|---|---|
| Re-run on the **same day**, same range — **measurements** | Same paths → **overwrites** (`open('w')` / `put_object`) | Yes |
| Re-run on the **same day** — **metadata** (`locations_index`, `sensors_by_location`, `sensors_index`) | Both backends **always overwrite** (**D3**, applied 2026-09-06). Until then `LocalStorage` skipped the write if the file existed | Yes |
| Run on **another day**, overlapping range | A different `ingest_date=` partition → the same measurement exists twice | Yes, by design |

> **Corrected in v4.** Overwriting was not uniform: locally, metadata was written *only if it did not exist*; on S3, always overwritten. The same logical operation behaved differently depending on the backend.
>
> **Resolved — D3 (§17): unify to "always overwrite".** Bronze's immutability is guaranteed by **partitioning by `ingest_date=`**, not by refusing to overwrite an individual file inside a partition. "Skip if exists" **breaks idempotency** (the result depends on whether you already ran that day) and **diverges between local and S3**, which contradicts the principle in **§6.1** (same artifact on laptop and in the cloud).
>
> **Applied on 2026-09-06** (#13). The three guards in `local_filesystem.py:29-45` are gone; the methods now mirror `s3_storage.py:44-60`. `S3Storage` was already correct and was not touched.
>
> **The decisive case is not theoretical.** `ZoneProcessor._process_sensors` swallows per-location errors and continues (`zone_processor.py:108-110`), so a run with API failures still writes `sensors_index.json` with an incomplete catalog — and reports success. Under "skip if exists" a corrective re-run **could not repair it**: the bad file stayed frozen until the `ingest_date=` partition rolled over the next day. On S3 the same scenario healed itself. Local could not be repaired and the cloud could — that is what D3 fixes. The silent failure that produces the incomplete catalog is a separate problem, tracked in **#17**.

The third case is the audit log working as intended. **Bronze is append-only and immutable.** If it is cleaned, replayability is lost, which is its only reason to exist.

> **Do not clean Bronze.** Deduplication belongs to Silver. That *is* the medallion architecture.

### 9.2 Where the real problem is

In how Silver reads. If Silver reads a single `ingest_date=` partition, it deduplicates within it but not against what it already wrote before.

### 9.3 Solution: Silver does a full rebuild

```
Silver reads ALL the ingest_date= partitions of the zone
  → global deduplication
  → writes event_date= partitions
```

**Idempotent by construction.** Running it five times gives the same result.

At 132 MB/month per city this takes seconds. Incremental mode (`--ingest-date`) is added later as an **optimization, not a requirement**. This is decision **D5** (§17), where the **Bronze vs Silver** comparison lives (immutability, nature, format) along with the path to incremental (watermark by `ingest_date`; Delta/Iceberg/Hudi at larger scale) consciously discarded for scale reasons. **Silver is derivable and disposable** — it is regenerated from Bronze — and that is why it **is not immutable**, unlike Bronze.

```bash
# default mode — always correct
python -m src.transformation.main --zone Guadalajara_Metropolitan --rebuild

# incremental mode — added later
python -m src.transformation.main --zone Guadalajara_Metropolitan --ingest-date 2026-07-22
```

### 9.4 Known edge case

If a re-run returns **fewer pages** than the previous one (e.g. 3 instead of 5), stale `page-4.json` and `page-5.json` files remain in the same partition.

Two ways out:
- **A (recommended):** ignore it. Silver's deduplication absorbs it.
- **B:** Bronze writes a `_manifest.json` with the page count; Silver reads only up to that.

> **Complement to D5 (§17).** On the **Silver** side the problem does not appear: a full rebuild with **overwrite by partition** rewrites the entire `event_date=` partition on every run, so **no orphan Parquet files** from previous runs survive. On the **Bronze** side, the stale `page-N` files remain (Bronze is append-only), but it is solved with option **A** — Silver's global deduplication absorbs them. Option **B** (`_manifest.json`) remains an alternative, not a necessity.

### 9.5 Silver write strategy

When writing by `event_date`, one run touches old partitions. With a full rebuild this solves itself: **all** the zone's partitions are rewritten on every run. No merge, no upsert logic, no state.

---

## 10. Per-stage tests

The **already-committed Guadalajara sample data is the fixture library.** That turns a previously questionable decision into an asset.

**`FakeStorage` is not used.** `LocalStorage` pointing at a temporary directory is already the test double — it runs the full pipeline without AWS, without credentials and at no cost. That is exactly why the abstraction was designed.

> **Suite born on 2026-09-06** with D3 (#13). Layout: `tests/` mirrors the stages
> of `src/` (`tests/ingestion/`, later `tests/transformation/`, `tests/aggregation/`),
> so each stage keeps its tests isolated — the same rule as **R1**. Tests live
> **outside** `src/` so they do not travel into the deployment artifact (#10).
> `pytest.ini` sets `pythonpath = .` because `src/` has no `__init__.py`; without
> it the suite passes under `python -m pytest` and fails under plain `pytest`,
> which would break CI (#11) for no real reason.
>
> First test in place: `tests/ingestion/test_local_storage_overwrite.py` — the
> Bronze row of the table below, in its metadata-writer slice.

| Stage | What is tested | How |
|---|---|---|
| **Bronze** | Given a simulated response, the correct paths are written | `LocalStorage(base=tmp_path)` |
| **Silver** | Given a real `page-1.json`, the flattened DataFrame has the expected columns, types and count | Pure function, no I/O |
| **Silver** | Deduplication: two overlapping partitions produce N unique rows | Pure function |
| **Gold** | Given a known Silver, the aggregations add up | Pure function |
| **RateLimiter** | Honors both windows under concurrency | `threading` + simulated clock |
| **paths.py** | Generated paths match the documented layout | Pure function |

**Silver's tests do not need Bronze to run.** That is what confirms the decoupling — if Bronze had to be executed to test Silver, they would still be tied together.

```python
# example — tests/test_silver.py
def test_flatten_produces_expected_columns():
    with open("bronze/zone=Guadalajara_Metropolitan/measurements/"
              "ingest_date=2025-11-22/sensor_id=22933/page-1.json") as f:
        page = json.load(f)

    df = flatten_measurements(page, zone="Guadalajara_Metropolitan",
                              sensor_id=22933)

    assert set(df.columns) >= {"zone", "sensor_id", "parameter", "value",
                               "units", "datetime_utc", "datetime_local",
                               "latitude", "longitude"}
    assert df["datetime_utc"].dtype.kind == "M"
    assert len(df) == len(page["results"])
```

> Reminder from v4: the measurements' `results[]` does **not** carry its own `id`; deduplication will be done by `sensor_id` + `period.datetimeFrom.utc` (see §11.2 and §4.5).

---

## 11. Pending decisions

> These remain **data domain** decisions (units, dedup, Gold grain). The **code architecture** decisions that arose from reading the real code are in §17.

### 11.1 Normalize units or preserve them?

There are ppm and µg/m³ mixed together (confirmed in the data: CO/NO/NO₂/NOx/O₃/SO₂ in ppm, PM10/PM2.5 in µg/m³).

| Option | In favor | Against |
|---|---|---|
| **Preserve** with a `units` column | Simple, honest, lossless | The consumer must handle the heterogeneity |
| **Normalize** to µg/m³ | Direct comparability | Requires molecular weight per pollutant, plus reference temperature and pressure |

**Technical note:** converting ppm → µg/m³ depends on temperature and pressure (typically 25 °C and 1 atm), and on the gas's molecular weight. It is not a single constant.

**Pending decision.**

### 11.2 Deduplication key

Hypothesis: `sensor_id` + `datetimeFrom.utc`, keeping the most recent by `ingest_date`.

**Checks against the raw data (some already answered in v4 with the sample data):**

- [x] **Is there any measurement `id` field in `results[]`?** → **No.** The keys of each `result` are `coordinates, coverage, flagInfo, parameter, period, summary, value`. This confirms the key must be built from `sensor_id` + `period.datetimeFrom.utc`.
- [ ] Is `period.datetimeFrom.utc` unique per sensor within a page? (the observed `interval` is `01:00:00` → hourly)
- [ ] Can `value` come back as `null`?
- [ ] Is the interval always hourly, or does it vary by sensor?
- [ ] Do two different `ingest_date` values return **different** values for the same `sensor_id` + timestamp? (if so, "most recent" is the right rule; if not, any of them works)

```bash
# commands to answer what is missing, locally
python - <<'EOF'
import json, glob
files = glob.glob("bronze/zone=Guadalajara_Metropolitan/measurements/**/page-*.json",
                  recursive=True)
print("files:", len(files))
d = json.load(open(files[0]))
print("meta:", d["meta"])
ts = [r["period"]["datetimeFrom"]["utc"] for r in d["results"]]
print("timestamps:", len(ts), "unique:", len(set(ts)))
print("nulls in value:", sum(1 for r in d["results"] if r.get("value") is None))
print("result keys:", sorted(d["results"][0].keys()))
EOF
```

**Pending decision.**

### 11.3 Gold grain

Not yet defined: a star schema (`fact_measurements` + dimensions) or wide aggregate tables? To be decided after Silver.

---

## 12. Decision history (Oct 2025 – Jul 2026)

### October 2025 — AWS fundamentals

- **Costs.** Monterrey ≈ 132 MB/month ≈ 4,500 files → **~$0.023 USD/month**. Four cities → **~$0.10 USD/month**. Conclusion: the cost of S3 is irrelevant and must not be a decision factor.
- **Bucket created manually from the console.** A deliberate decision: separate infrastructure from logic and learn the console. IaC was left for the future.
- **Partitioning validation.** It was confirmed that the Hive-style design with event/ingest separation is the pattern used by Netflix, Uber and Airbnb.
- **The `zone=` level was added** to scale to multiple cities.
- **Python vs Glue for Silver.** Glue wins on scale and brings Data Catalog + crawlers, but it is expensive for small volumes. Python wins on cost, control and local debugging. **Decision: start in Python.**
- **Lambda's limit.** A sequential zone takes ~18 min (300 sensors × 3 pages × 1.2 s), over the 15-min timeout. This ruled out Lambda for monolithic ingestion.

### November 2025 — Architecture and release

- **Pattern study:** Repository, Hexagonal (Ports & Adapters), Clean Architecture → the origin of `StorageInterface`.
- **10 parallelism options evaluated:** SQS + Lambda with reserved concurrency, Step Functions with Map State, ECS Fargate, MWAA/Airflow (rejected: ~$300/month), Glue Python Shell Jobs.
  - Recommended at the time: **SQS + Lambda with reserved concurrency of 10–15** → from 45 min to ~10–15 min.
  - **Not implemented.** The pipeline is still sequential — slow but it honors the rate limit.
- **ADF vs Databricks** analyzed. It was clarified that Dataflow Gen2 belongs to Microsoft Fabric, not to ADF.
- **Nov 22, 2025 — Release v1.0.0.** Last commit.

### December 2025 – January 2026 — Silver design

- **Silver layer schema defined: 43+ fields.**
- **dbt / Dagster vs native AWS.** They are not mutually exclusive: AWS is infrastructure, dbt is versioned SQL transformation, Dagster is orchestration. Typical architecture: S3 → ingestion → Athena/Redshift → dbt → Dagster.

### February 2026 — Containers (planned, not executed)

- **Decision:** write the Silver script and bring it up in Docker.
- Silver is batch, isolated and stateless → an ideal container case.
- Path: local container → ECS Fargate / cron / manual.
- **Orchestration clarified:** the orchestrator does not live inside the containers, it directs them from the outside.
  ```bash
  0 2 * * * docker run bronze-etl && docker run silver-etl
  ```
- **`src/main.py` requires no changes**; only a second entry point is added.
- **Actual result: nothing was committed.**

### July 2026 — Reframing and unblocking

- The real state was documented and the phased plan was produced.
- It was mapped against active job openings (Arca Continental, Banregio, Banorte, Microsoft).
- **IAM Roles vs access keys** studied (Jul 15) — applicable to the security debt.
- **Jul 21 — the root cause of the block was identified:** `StorageInterface` without read methods. See §5.1.
- **Jul 21 — rate limits confirmed** by the author: 60/min and 2,000/hour.
- **Jul 21 — decisions made:** monorepo with three images, GitHub Actions as the initial orchestrator, `event_date` for Silver, same bucket with separate prefixes, tests will be done, `FakeStorage` discarded.
- **Jul 30 — verification by direct code reading** (v4): data schemas and the real `StorageInterface` contract were corrected; decisions D1–D4 arose (§17).

---

## 13. Strategic decisions (Jul 2026)

### 13.1 Tools

| Tool | Verdict | Reason |
|---|---|---|
| **Claude Code** | **Now** | It attacks the real bottleneck: the friction of sitting down to write. Microsoft asks for it verbatim in *Preferred Qualifications* ("agentic plugins, agents, skills, hooks"). Caution: do not spend a week configuring it. First prompt = `silver_transformer.py` |
| **Skills** | **Minimal version** | A project skill that encodes the conventions: Hive-style partitioning, the `StorageInterface` contract, commit style, the rule that every new storage implements the ABC. ~30 min. Prevents Claude Code from proposing solutions that break the architecture |
| **AI agents** | **Deferred** | An agent today would query raw, uncleaned JSON. **It requires Gold to exist.** It is the differentiator against Microsoft, but in Phase 4 |
| **IaC (Terraform)** | **After Silver** | Terraform over Bicep: it works on both clouds and is more marketable. Apply it to real resources (bucket, roles, policies), not to exercises |
| **Airflow** | **Phase 6, containerized** | Confirmed learning goal. Cheap migration because the pipeline is already containerized. See §7.8 |
| **Self-hosting** | **As a lab** | Docker + Postgres + **MinIO** (S3-compatible: `S3Storage` works unchanged) + Dagster = an unlimited lab at zero cost. Caution: nobody asks "have you run Airflow on Docker?" — they ask about Databricks, ADF, Fabric. It is a practice field, not a portfolio |

### 13.2 "100% cloud project" — a questioned premise

**"100% cloud" is not a goal, it is a self-imposed constraint.** No real team works that way: they develop locally and deploy to the cloud.

A pipeline that runs identically on the laptop and in the cloud — exactly what `StorageInterface` already enables — is more professional than one that only runs on AWS, and it solves the cost problem along the way.

### 13.3 AWS Glue — real numbers

| Component | Cost |
|---|---|
| **Data Catalog** | First million objects and requests **free per month** |
| **Spark ETL job** | $0.44/DPU-hour, minimum 2 DPU → **~$0.88/hour**. A 15-min job with 6 DPU = $0.66 |
| **Python Shell ETL job** | Can use 0.0625 DPU → **~$0.003/hour**. Practically free at this volume |
| **Interactive Sessions** | 5 DPU by default → **$0.88 per 24 minutes** |
| **Development endpoints** | 10-min minimum and **no automatic timeout** — the classic first-month mistake |

**There is no free tier for ETL jobs.** Only the Data Catalog has one.

**Conclusion:** Glue is not expensive to **run** (a 10-min Python Shell job costs fractions of a cent). It is expensive to **develop** — iterating in an interactive notebook is 88 cents every 24 minutes.

**Strategy:** write and debug Silver locally with pandas against the Guadalajara data. Once the logic works, porting it to a Glue job is a matter of wrapping the same function. **The transformation logic is portable; the runtime is a deployment detail.**

And this reinforces R2: if Silver depends on Glue to exist, it is not decoupled — it is tied to AWS.

### 13.4 Cloud strategy

**AWS**
- The data lake costs cents. If credits are being consumed, **it is almost certainly not S3** — it is an EC2 left running, a NAT Gateway, or an idle managed service.
- **Action:** open Cost Explorer and **filter by service** before any cost-based architecture decision.
- Account created ~Oct 2025 → 12-month free tier valid until ~Oct 2026.

**Azure**
- $200 USD of credit for the **first 30 days**, plus 12 months of selected free services.
- Unused credit **is lost**. To keep the 12 months you must move to pay-as-you-go by removing the spending limit within those 30 days.
- **Do not create the account yet.** The clock starts at registration, not at the first deployment.
- **Create it when Silver and Gold are ready** and there is a written spending plan. The $200 is enough for one intensive month of **ADF + Fabric + Azure OpenAI** — the stack Arca and Microsoft ask for and that cannot be replicated locally. Spending it on storage or VMs would be a waste.

**Do not migrate the project to Azure.** But do add **one** `AzureBlobStorage` class implementing `StorageInterface`. That is not a migration — it is the proof that the pattern was worth it. Half an hour of work. *(Note: which methods it must implement depends on D1 — see §17.)*

### 13.5 Own decisions, questioned

| Decision | Analysis |
|---|---|
| **One JSON per page** (~4,500 files/month/city) | *Small files problem*: it kills the performance of Spark and Athena, which spend more time opening files than reading them. Defensible in Bronze (immutability, replay), but you must be able to name the trade-off. Consider consolidating into one JSONL per sensor/day |
| **Data committed to the repo** | Not what you would do in production and the repo grows. **But** it is the fixture library for tests (§10), which makes it defensible. Make it deliberate and explained in the README |
| **Sequential pipeline** | Not an omission, a decision: it honors the rate limit. Present it that way |

---

## 14. Execution plan

### Guiding principle

> **Do not start a new project.** This repo has 78 commits, a decoupled architecture, real data and serious documentation. Finishing it is worth more than starting two more.

### Phase 0 — Hygiene (1 hour)

Breaks the 8-month streak and removes the README's credibility risk.

1. **AWS Cost Explorer**, filter by service → a real diagnosis of consumption
2. Fix the README: remove "production-ready", change to "Bronze layer complete — Silver/Gold in progress", remove `tests/` from the documented structure
3. Add a **Roadmap** section with the real state per layer
4. Create issues: `Silver layer`, `Gold layer`, `Storage read methods`, `Dockerize`, `CI pipeline`, `Rate limiter`
5. Commit and push

### Phase 1 — Unblocking and Silver (week 1)

| # | Deliverable |
|---|---|
| 1 | `read_bytes`, `save_bytes`, `list_paths`, `exists` (+ `save_json` helper) as the **complete contract** of `StorageInterface`; relocate the semantic `save_*` methods (incl. `save_measurements_raw`) to the ingestion layer on top of `paths.py` (**D1 resolved** → §5.5) |
| 2 | `src/common/paths.py` |
| 3 | `silver_transformer.py` — read, flatten, clean, deduplicate, write Parquet (full rebuild, overwrite by partition — **D5**) |
| 4 | `src/transformation/main.py` with a CLI (`--zone --rebuild --storage`) |
| 5 | Run it against Guadalajara locally |
| 6 | Remove the latent `event_date` logic from `ingestion/` (**D2 resolved**; recover from git `1062792` as a seed) |

**Estimate: 4–6 hours.** Step 1 is what was stuck for 8 months and it is ~40 lines.

### Phase 2 — Containers and CI (week 2)

| # | Deliverable |
|---|---|
| 1 | `requirements/` split per stage |
| 2 | `docker/bronze.Dockerfile` and `docker/silver.Dockerfile` |
| 3 | `.dockerignore` |
| 4 | Tests: Bronze (paths), Silver (flattening, dedup), `paths.py` |
| 5 | GitHub Actions: tests on every push |

### Phase 3 — Orchestration and rate limiting (week 3)

| # | Deliverable |
|---|---|
| 1 | Dual-window `RateLimiter` + its tests (integration with `http_client` → **D4**) |
| 2 | `ThreadPoolExecutor` in `ZoneProcessor`, isolated per-sensor failure |
| 3 | Confirm `exist_ok=True` in `LocalStorage`'s directory creation under threads |
| 4 | `bronze-daily` workflow with matrix, `fail-fast: false`, `max-parallel: 1` |
| 5 | Separate `catalog-weekly` workflow |

### Phase 4 — Gold (weeks 4–5)

- Star schema: `fact_measurements` + `dim_sensor`, `dim_location`, `dim_parameter`, `dim_date`
- Aggregates by parameter / zone / day / hour
- `docker/gold.Dockerfile` and its entry point
- Optional: AQI index with EPA standards

### Phase 5 — Differentiator (weeks 6–7)

Pick **one** according to the goal:

| Goal | Differentiator |
|---|---|
| Arca / Banregio | dbt + Snowflake (free trial) |
| Microsoft | AI layer with Azure OpenAI on top of Gold (text-to-SQL or RAG) |
| Generic Data Engineer | Orchestration with Dagster |

**This is where the Azure account is created**, not before.

### Phase 6 — Containerized Airflow (weeks 8–9)

> Confirmed learning goal. See §7.8 for the technical detail.

**Prerequisite: Gold must exist.** Before that there is no dependency graph to orchestrate.

| # | Deliverable |
|---|---|
| 1 | `airflow/` folder with the official `docker-compose.yaml` |
| 2 | Bring up the local stack: scheduler, webserver, Postgres |
| 3 | Mount `/var/run/docker.sock` to enable `DockerOperator` |
| 4 | `dags/airq_pipeline.py` with the `catalog → bronze → silver → gold` chain |
| 5 | `max_active_tasks=1` — same reason as `max-parallel: 1` (§7.2) |
| 6 | Test **native backfill** over a historical range with `catchup=True` |
| 7 | Retries with backoff per task |
| 8 | Document the migration in the README: why Actions first, why Airflow later |

**A rule that does not get broken:** zero business logic in the DAG. Only `docker run` with parameters.

**Honest note for the README and the interview:** at this scale GitHub Actions is more than enough. The migration is for learning and employability, not technical necessity. Saying it that way is worth more than pretending it was needed.

### Phase 7 — Presentation (week 10)

- PySpark: rewrite Silver documenting the why
- Power BI dashboard on top of Gold → closes ingestion → transformation → modeling → visualization
- Terraform over the real AWS resources
- Updated architecture diagram + a "Results" section with numbers
- Release **v2.0.0**

### Cadence

- Small, descriptive commits
- One issue per deliverable, closed with its PR
- Milestones per phase → this *is* demonstrable agile methodology
- **Commit at least every 60 days** or the scheduled workflows get disabled

---

## 15. Confirmed checklist

### Architecture
- [x] Monorepo, three independent images, same artifact on laptop / Actions / ECS
- [ ] `StorageInterface` = **4 primitives** (`read_bytes`/`save_bytes`/`list_paths`/`exists`) + `save_json` helper; the semantic `save_*` methods (incl. `save_measurements_raw`) move up to the ingestion layer on top of `paths.py` (**D1 resolved**, §5.5 / §17)
- [x] `src/common/paths.py` as the shared contract
- [x] Three entry points, each with its own CLI
- [ ] Zero cross imports/logic between stages (today there is `event_date` logic in `ingestion/`; **D2 resolved**: remove it, git ref `1062792`)
- [x] Silver and Gold in the same bucket / root folder, separated by prefix

### Docker
- [x] `docker/bronze.Dockerfile` and `docker/silver.Dockerfile`
- [x] `requirements/` split per stage
- [x] `.dockerignore`
- [x] `ENTRYPOINT` with passable args

### Orchestration
- [x] GitHub Actions, daily cron at 2am, `workflow_dispatch` for backfills
- [x] **build-once-push** pattern to `ghcr.io` with `needs:` (§7.4)
- [x] `fail-fast: false` + `max-parallel: 1`
- [x] Do **not** parallelize zones — it does not help, the ceiling is the API (§7.3)
- [x] Separate weekly workflow for the catalog
- [x] Secrets in GitHub Secrets
- [x] Periodic commit so the cron is not disabled after 60 days
- [x] **Zero business logic in the orchestrator** — protects portability

### Airflow (Phase 6)
- [x] Containerized with the official `docker-compose.yaml`
- [x] `DockerOperator` + mounted Docker socket
- [x] `max_active_tasks=1` for the same reason as `max-parallel: 1`
- [x] `catalog → bronze → silver → gold` DAG
- [x] Test native backfill with `catchup=True` and `{{ ds }}`
- [x] Prerequisite: **Gold must exist**
- [ ] *Optional later:* try Dagster (same containers, the decision is not a lock-in)

### Rate limiting
- [ ] Dual-window `RateLimiter`, 55/min and 1900/hour (**D4 resolved**: replace `sleep_by_rate`; rewrite fetcher signatures; prerequisite for `ThreadPoolExecutor`)
- [x] `acquire()` before every request, **including pagination**
- [x] `ThreadPoolExecutor(max_workers=4)` within the zone
- [x] A failing sensor does not take down the zone
- [x] Idempotent directory creation in `LocalStorage` (`Path.mkdir(exist_ok=True)` — already present)
- [ ] Verify `boto3` thread-safety with a real run

### Data
- [x] Bronze stays **raw and immutable** — it is not cleaned
- [x] Silver partitions by `event_date`
- [x] Silver does a **full rebuild** by default, **overwrite by partition** → idempotency by construction (**D5**)
- [x] Incremental `--ingest-date` mode as a future optimization (watermark; Delta/Iceberg/Hudi at larger scale — **D5**)
- [x] Unify metadata write semantics to "always overwrite" (fix `LocalStorage`) — **D3 implemented 2026-09-06**

### Tests
- [x] They will be done (author's decision)
- [x] No `FakeStorage` — `LocalStorage(tmp_path)` fills that role
- [x] Bronze: correct paths
- [x] Silver: flattening and dedup over the Guadalajara JSON files
- [x] `RateLimiter`: both windows under concurrency
- [x] `paths.py`: paths match the layout

### Pending decision
- [ ] Normalize units vs preserve them (§11.1)
- [ ] Deduplication key (§11.2 — partially resolved: there is no measurement `id`)
- [ ] Gold grain (§11.3)
- [x] Code architecture: **D1–D5 resolved** (§17) — implementation in `src/` pending

### Deferred
- [ ] Glue (port Silver later, do not develop there)
- [ ] Azure (create the account in Phase 5)
- [ ] IaC / Terraform (Phase 6)
- [ ] AI agents (requires Gold)
- [ ] PySpark (Phase 6)
- [ ] `AzureBlobStorage` as a demonstration of the pattern

---

## 16. References

### Direct verification
- **v4 (Jul 30, 2026): direct reading of the source code on the local filesystem.** The following were verified: `storage_interface.py`, `local_filesystem.py`, `s3_storage.py`, `zone_processor.py`, `orchestrator.py`, `http_client.py`, `fetchers.py`, `settings.py`, `argument_parser.py`, `config_loader.py`, `helpers.py`, `output_formatter.py`, `zones_config.json`, `requirements.txt`, and real samples of `locations_index.json`, `sensors_by_location/*.json`, `sensors_index.json` and `page-1.json`. Sections §2–§5, §8, §9 and §11 were corrected against the real code; the architecture decisions that arose are in §17.
- v3 and earlier: GitHub repository, fetched on **July 21, 2026** (README, root listing, releases, language statistics). Warning: in v3 it was **not possible to read the source code** (GitHub was blocking automated access to `/tree/` and `/blob/`), so §5 and §9 of v3 were reasoning about the README, not reading the files. **v4 resolves that limitation.**

### OpenAQ rate limits
- Confirmed by the author with a screenshot from Jul 21, 2026: **60 req/min, 2,000 req/hour**, tied to the personal API key. Higher limits available under agreement with OpenAQ.

### AWS Glue pricing
- Checked on Jul 21, 2026. ETL jobs at $0.44/DPU-hour with a 2-DPU minimum; Data Catalog with the first million objects and requests free; Python Shell jobs from 0.0625 DPU; interactive sessions with 5 DPU by default; development endpoints with no automatic timeout.

### Azure free account
- Checked on Jul 21, 2026: $200 USD of credit for 30 days plus 12 months of selected services; unused credit is lost and the spending limit must be removed within the 30 days to keep the 12 months.

### Conversations consulted (Oct 2025 – Jul 2026)

| Date | Topic | Contribution |
|---|---|---|
| Oct 2025 | AWS IAM and pricing | S3 costs, lake structure, Python vs Glue, Lambda limits, manual bucket |
| Nov 2025 | Architecture patterns | Repository / Hexagonal / Clean → `StorageInterface` |
| Nov 2025 | ADF and Databricks | 10 parallelism options, SQS+Lambda, ECS Fargate, MWAA rejected |
| Dec 2025 | dbt and Dagster vs AWS | Separation of infrastructure / transformation / orchestration |
| Dec 2025 – Jan 2026 | Silver design | 43+ field schema |
| Feb 2026 | Containerizing Silver | Docker, ECS Fargate, external orchestration, GitHub Actions |
| Apr 2026 | Résumé update | Project write-up, portfolio inventory |
| Jul 2026 | IAM Role vs IAM User | Security debt |
| Jul 2026 | Career plan | Real state per layer, phased plan |
| Jul 2026 | Arca / Banregio / Banorte / Microsoft openings | Requirements mapping |
| Jul 21, 2026 | Unblocking the project | Root cause, decoupling rules, per-stage Docker, orchestration, rate limiting, idempotency |
| **Jul 30, 2026** | **Audit of the real code (v4)** | **Real `StorageInterface` contract, data schemas, undocumented code, decisions D1–D4** |

---

## 17. Pending decisions (Jul 2026 — v4)

> These decisions arose from **reading the real code** and contrasting it with what the documentation assumed. They are not facts to correct (those were already corrected in the text above): they are **design choices** that are yours to make. Each has a stable ID (D1, D2, …) so you can answer in future messages with "D1: option B" or similar.
>
> **Update 2026-08-03.** D1–D4 resolved by the author; **D5** was added (also resolved) about Silver's reprocessing strategy. The **My decision** field of each one reflects the choice made. The affected sections were adjusted accordingly: §3.2 and §5.5 (D1), §9.1 (D3), §8.0/§8.3 (D4), §9.3–§9.4 (D5).
>
> **Update 2026-09-06.** **D3 is implemented** (#13). **D1, D2, D4 and D5 are still only decisions** — the code has not been touched for them.

### D1 — Scope of the `StorageInterface` contract

- **What I found:** The `storage_interface.py` ABC declares only **two** `@abstractmethod`s: `save_json` and `save_measurements_raw`. The methods `save_locations_index`, `save_sensors_by_location`, `save_sensors_index` and the path helpers (`zone_dir`, `metadata_dir`, `measurements_dir`) exist only in `LocalStorage` and `S3Storage`. `ZoneProcessor` calls `save_locations_index`, `save_sensors_by_location` and `save_sensors_index`, which are **not** in the contract: today it works by *duck-typing*.
- **Why it is a decision and not a fact:** The fact is "the contract has 2 methods". What to do about the gap between the contract and what `ZoneProcessor` actually uses is a choice with consequences: a new backend (`AzureBlobStorage`, `GCSStorage`, MinIO) could be instantiated while satisfying the ABC and still **break at runtime** for lacking methods the interface does not demand. It also conditions the read methods of §5.5.
- **Options:**
  - **Option A — Widen the ABC now:** declare as abstract every method `ZoneProcessor` uses (`save_locations_index`, `save_sensors_by_location`, `save_sensors_index`) plus the read methods of §5.5 (`read_bytes`, `save_bytes`, `list_paths`, `exists`). *(Trade-off: the contract becomes the single source of truth and an incomplete backend fails **at construction**, not in production; but it forces implementing everything in every adapter and makes adding a new one expensive.)*
  - **Option B — Keep the contract minimal:** keep only the 2 abstract methods and let `ZoneProcessor` go on calling methods outside the ABC (*duck-typing*). *(Trade-off: less friction for prototyping, but the abstraction "lies" — the polymorphism promise of §3.2 is not guaranteed and errors show up late, at runtime.)*
  - **Option C — Split the contract by role:** separate into small interfaces (`MetadataWriter`, `MeasurementsWriter`, `Reader`) and compose. *(Trade-off: cleaner and it fits R2/R3, but it is more scaffolding than a single-developer project usually needs.)*
- **My decision: Option D — cut down to domain-agnostic primitives** (none of A/B/C as stated).
  - **The contract (`@abstractmethod`) contains ONLY primitives agnostic to the data source:** `save_bytes`, `read_bytes`, `list_paths`, `exists`. `save_json` is kept as a **concrete helper built on top of `save_bytes`** (sugar, not a new primitive). The target contract ends up at **4 primitives + `save_json`**.
  - **The semantic OpenAQ methods leave the contract and the backend.** `save_locations_index`, `save_sensors_by_location`, `save_sensors_index` — and, by the same rule, `save_measurements_raw`, which today is an `@abstractmethod` but speaks of `sensor_id`/`pages_data`/`ingest_date` — **relocate one level up**, into the ingestion layer, built on the primitives and with **paths computed by `src/common/paths.py`**.
  - **Reason:** "locations" and "sensors" are **OpenAQ vocabulary**. A storage adapter should know nothing about the domain. With this cut, `AzureBlobStorage`, `MinIO` or `GCSStorage` implement **4 generic methods** (5–6 counting helpers) and serve equally for OpenAQ, for **Silver** (which writes Parquet, not "locations") and for a **future source** (e.g. NASA) without touching the interface.
  - **Why it is better than the original Option A (widen the ABC with every method):** Option A does make the contract the single source of truth, but it **bakes the OpenAQ domain into the storage port**. Every new backend would be forced to implement `save_locations_index` even when its real consumer is Silver, which has no "locations". That is exactly the coupling Ports & Adapters aims to avoid: the secondary (*driven*) adapter, the storage, would end up knowing the domain of the primary adapter (OpenAQ ingestion). Cutting to primitives keeps the **port narrow and stable**, and leaves domain knowledge where it belongs: in the stage, on top of `paths.py`.
  - **Against B** (minimal + duck-typing): it resolves the same contract "lie", but **without** leaving methods outside the ABC called by convention — the semantic ones stop being the backend's responsibility. **Against C** (split by role): simpler; separate `MetadataWriter`/`Reader` are unnecessary when 4 primitives cover everything.
  - Detail and implications in **§3.2** and **§5.5**.

### D2 — Silver-style logic inside `ingestion/`

- **What I found:** `LocalStorage` implements `measurements_event_date_dir()` and `save_measurements_by_event_date()` (writes JSONL grouped by `event_date` under `measurements/event_date/year=/month=/day=/`), and `ZoneProcessor` has `_organize_by_event_date()`. Today it is **disabled**: in `_process_measurements` those lines are commented out and the active flow writes only raw Bronze (`save_measurements_raw`). In other words: there is Silver-style transformation already written, latent, living inside the ingestion layer.
- **Why it is a decision and not a fact:** The fact is "that code exists and is commented out". Keeping it, removing it, or migrating it collides directly with **R1** (§5.2: no stage mixes another's responsibilities) and with R2 (the contract is the data at rest). Keeping transformation in `ingestion/` violates R1 even while switched off, and the document cannot claim full decoupling while it stays there.
- **Options:**
  - **Option A — Remove it from `ingestion/`:** leave the ingestion layer 100% pure Bronze. *(Trade-off: full coherence with R1; already-written code is "lost", although it remains in git history to recover it.)*
  - **Option B — Extract it to `src/transformation/` as a Silver seed:** move it when that layer is created. *(Trade-off: reuses the work and respects R1; but it must be adapted to the read contract of §5.5 — which does not exist yet — and to the Parquet format of §4.4, since today it writes JSONL.)*
  - **Option C — Leave it latent where it is:** touch nothing for now. *(Trade-off: zero immediate work, but `ingestion/` keeps violating R1 and the document's decoupling would be aspirational, not real.)*
- **My decision: remove it from `ingestion/` + record the reference in git** (Option A "remove", with an explicit safety net).
  - The Silver-style code is **removed** from the ingestion layer: `LocalStorage.measurements_event_date_dir()`, `LocalStorage.save_measurements_by_event_date()` and `ZoneProcessor._organize_by_event_date()` (plus the two commented-out lines that invoked it in `_process_measurements`). Bronze stays **100% pure** and `ingestion/` satisfies **R1**.
  - **Safety net — git reference** (to recover it as a starting point when building Silver, without retyping it from memory):

  | What | Where |
  |---|---|
  | Last commit where the code **exists** (before removing it) | **`1062792`** (HEAD as of 2026-08-03) |
  | Commit that **introduced** it | **`e278a88`** — *"refactor!: restructure project with clear separation of concerns"* (2025-11-21) |
  | `_organize_by_event_date()` | `src/ingestion/openaq/pipeline/zone_processor.py` (method ~L173; commented calls L150–151) |
  | `measurements_event_date_dir()` | `src/ingestion/openaq/storage/local_filesystem.py` (~L66) |
  | `save_measurements_by_event_date()` | `src/ingestion/openaq/storage/local_filesystem.py` (~L81) |

  Recovery: `git show 1062792:src/ingestion/openaq/storage/local_filesystem.py`.

  - **It will not be copied as-is.** It serves as a **seed** for the `event_date` grouping logic in Silver, but it must be **rewritten**:
    - today it writes **JSONL**; Silver writes **Parquet (snappy)** (§4.4);
    - today it uses `LocalStorage`'s direct I/O; Silver must lean on the **read/write contract of §5.5** (D1's primitives) to be backend-agnostic;
    - it **does not deduplicate or overwrite by partition**; Silver does — full rebuild with *overwrite by partition* (see **D5**).
  - **Explicit clarification (important):** **Bronze stores raw JSON exactly as the API returns it — NOT JSONL.** The JSONL grouped by `event_date` is **transformed output and belongs to Silver by definition**. Bronze does **not** partition by `event_date`; it partitions **only** by `zone/ingest_date/sensor_id`. That this code wrote JSONL grouped by `event_date` inside `ingestion/` is precisely the R1 violation that D2 fixes.

### D3 — Metadata write semantics (local vs S3)

- **What I found:** For the metadata files (`locations_index`, `sensors_by_location`, `sensors_index`), `LocalStorage` **does not overwrite**: if the file already exists, it skips and returns `False`. `S3Storage`, in contrast, **always** overwrites (unconditional `put_object`, returns `True`). The same logical operation behaves differently depending on the backend.
- **Why it is a decision and not a fact:** The fact is "the two backends diverge". Which behavior is correct — and unifying it — is a choice that affects idempotency (§9) and re-execution: should re-running a day **refresh** the metadata or **preserve** the first version?
- **Options:**
  - **Option A — Unify to "always overwrite" (like S3):** *(Trade-off: simple, predictable idempotency on both sides; you lose the "do not clobber" protection if two runs on the same day return different catalogs.)*
  - **Option B — Unify to "write-if-not-exists" (like local):** *(Trade-off: the day's first version stays immutable, good for auditing; but on S3 you must add a prior `head_object`, and a run with a corrected catalog would not be reflected without deleting first.)*
  - **Option C — Make it configurable** (an `--overwrite-metadata` flag with an explicit default): *(Trade-off: flexible and honest, but it adds CLI surface and one more branch to test.)*
- **My decision: Option A — unify to "always overwrite" on both backends.** ✅ **Implemented on 2026-09-06** (#13): the three guards in `local_filesystem.py:29-45` removed, covered by `tests/ingestion/test_local_storage_overwrite.py`.
  - **`LocalStorage` was fixed** so it **stops skipping** when the metadata file already exists (it used to return `False` and not rewrite). `S3Storage` already behaved that way (unconditional `put_object`); it was the one that was right, and it was not touched.
  - **Reason (documented in §9):** Bronze's immutability comes from **partitioning by `ingest_date=`**, not from refusing to overwrite an individual file inside a partition. "Skip if exists":
    - **breaks idempotency** — the result of a run depends on whether you already ran that day;
    - **diverges between local and S3**, which contradicts the principle of **§6.1** ("it is not Docker on my laptop *or* ECS; it is the same artifact running on both sides").

### D4 — Existing rate limiter vs the proposed dual window

- **What I found:** §8.3 proposed a dual-window `RateLimiter` (deque + lock, shared across threads) as if there were nothing there. But real rate limiting already exists in `http_client`: `sleep_by_rate()` (reactive, based on the `x-ratelimit-remaining`/`x-ratelimit-reset` headers) and `get()` with `429` retry (`max_retries=5`). It is per-process global and embedded in every `get()`, **not** coordinated across threads.
- **Why it is a decision and not a fact:** The fact is "a reactive limiter already exists". Replacing it, combining it, or keeping it is a design choice — especially because the plan in §8.4 (`ThreadPoolExecutor`) needs cross-thread coordination that the current `sleep_by_rate` does not provide.
- **Options:**
  - **Option A — Replace** `sleep_by_rate` with the proactive dual-window `RateLimiter` of §8.3 and call `acquire()` before every `get()`. *(Trade-off: correct under concurrency and with an explicit minute+hour budget; forces rewriting the fetcher signatures to pass the limiter, §8.5.)*
  - **Option B — Keep the reactive header-based approach** and just make it thread-safe. *(Trade-off: less change and it uses real server information; but it reacts late — you already sent the request that went over the limit — and it does not model the hourly window.)*
  - **Option C — Combine:** a proactive dual-window limiter as the first line + reading the header as verification/adjustment (exactly what §8.3 called an "optional improvement"). *(Trade-off: the most robust; also the most code and the most to test.)*
- **My decision: Option A — replace `sleep_by_rate` with the proactive dual-window `RateLimiter` (§8.3).**
  - `acquire()` **before every request, including pagination** (§8.5), not once per sensor.
  - **It implies rewriting the fetcher signatures** to receive the `limiter` (`fetch_locations_bbox`, `fetch_sensors_by_location`, `fetch_measurements_for_sensor_raw`, and `http_client`'s `get()`) — today rate limiting lives embedded in `get()` and the fetchers receive no limiter (see the note in §8.4).
  - **It is a prerequisite for `ThreadPoolExecutor` (§8.4):** the current `sleep_by_rate` is reactive and **per-process global**, embedded in every `get()`, and it **does not coordinate the budget across threads**. Without this replacement, parallelism would trigger 429s instead of speeding things up.

### D5 — Silver reprocessing strategy (resolved)

> Unlike D1–D4, this one did not arise from a code ambiguity: it documents the **reason** for a decision already made, so that the why is recorded rather than the question.

- **My decision: full rebuild, not incremental.** Silver reads **all** the `ingest_date=` partitions of the zone, **deduplicates globally** and **fully rewrites** the `event_date=` partitions — **overwrite by partition, not by file**.
- **Why:**
  - At **~132 MB/month per city** the rebuild **takes seconds**; **idempotency by construction** (running it five times gives the same result) is worth more than incremental optimization.
  - **Overwrite by partition** (not by file) eliminates orphan files **on the Silver side**: by rewriting the whole `event_date=` partition on every run, no Parquet files from previous runs survive. *(For the orphan `page-N` files **on the Bronze side** in §9.4, the absorber is still Silver's global deduplication — see §9.4·A; D5 closes the gap on the transformed side.)*
- **Silver is derivable and disposable:** it is regenerated from Bronze at any time. That is why it **is not immutable**, unlike Bronze, which **is** the source of truth. Comparison:

  | | **Bronze** | **Silver** |
  |---|---|---|
  | Nature | Source of truth | Derived / disposable |
  | Immutability | Immutable, append-only | Rewritten on every rebuild |
  | Format | Raw JSON from the API | Parquet (snappy) |
  | Partition | `zone/ingest_date/sensor_id` | `zone/event_date` |
  | Duplicates | Allowed (across `ingest_date=`, by design) | Eliminated (global dedup) |
  | Regenerable | No — it is the origin | Yes — from Bronze |

- **Path to incremental (known and discarded for scale, not omitted):**
  - **Watermark by `ingest_date`** — the `--ingest-date` flag of §9.3 would process only new partitions. It is added as an **optimization**, not a requirement.
  - At larger scale, **table formats with `MERGE`/upsert**: **Delta Lake, Apache Iceberg, Apache Hudi**, which support overwrite/merge by partition and *time travel* — something **plain Parquet does not have**. It is **consciously** discarded at this volume: the full rebuild is simpler and more correct. It is not an omission; it is an alternative evaluated and discarded for scale.

---

*Document v4.1 — updated on September 6, 2026. First version verified by direct reading of the source code. The objective corrections are applied in the text and the five architecture decisions of §17 (**D1–D5**) were **resolved** at the design level. **`src/` was modified for the first time since November 2025:** the `AWS_S3_BUCKET_NAME` message (#15) and **D3** (#13), which also brings the first tests into the repository. **D1, D2, D4 and D5 remain pending in the code** — trimming the `StorageInterface` contract (D1), removing the latent code from `ingestion/` (D2) and replacing `sleep_by_rate` (D4) are later work not yet executed. The rest consolidates decisions recorded between October 2025 and August 2026.*
