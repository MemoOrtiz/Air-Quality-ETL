# Issues — Phase 0

> **Status: created.** All ten issues below live on GitHub as **#6–#15**, and
> every label listed at the end exists in the repo. This file is kept as the
> written record of their scope and acceptance criteria — the place to read what
> an issue actually means without leaving the repository.
>
> **Progress (2026-09-06).** **#15** and **#13** are implemented on `dev` and
> waiting for the pull request into `main` that will close them. **#17** was
> added after Phase 0 was drafted; it is not part of the original ten.
>
> The `§` references point to `docs/AirQuality_ETL_Master_Document.md`.
> Labels in parentheses on each title.

| Issue | Title | Decision |
|---|---|---|
| [#6](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/6) | Storage: trim the contract to primitives | **D1** |
| [#7](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/7) | Create `src/common/paths.py` | — |
| [#8](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/8) | Silver layer — transformation | **D5** |
| [#9](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/9) | Gold layer — modeling | — |
| [#10](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/10) | Dockerize per stage | — |
| [#11](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/11) | CI pipeline (GitHub Actions) | — |
| [#12](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/12) | Proactive dual-window `RateLimiter` | **D4** |
| [#13](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/13) | Unify metadata writes to "always overwrite" | **D3** |
| [#14](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/14) | Remove latent Silver-like code from `ingestion/` | **D2** |
| [#15](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/15) | Bug: error message names the wrong env var | — |
| [#17](https://github.com/MemoOrtiz/Air-Quality-ETL/issues/17) | Structured logging and visible swallowed errors | — |

**Order of attack** (lowest to highest risk, one per session, per `CLAUDE.md`):
#15 → #13 → #14 → #6 + #7 → #8.

---

## #6 — Storage: trim the contract to primitives + read methods (D1) `(enhancement, architecture)`

**What it involves.** Trim `StorageInterface` to domain-agnostic primitives
(`save_bytes`, `read_bytes`, `list_paths`, `exists`) with `save_json` as a helper
on top of `save_bytes`. OpenAQ's semantic methods (`save_locations_index`,
`save_sensors_by_location`, `save_sensors_index`, and `save_measurements_raw`)
move out of the backend and up to the ingestion layer, with paths computed by
`src/common/paths.py`. This is the unblock that lets Silver read Bronze.

**Acceptance criteria**
- [ ] The ABC declares exactly `save_bytes`, `read_bytes`, `list_paths`, `exists`
- [ ] `save_json` is a concrete helper on top of `save_bytes`
- [ ] `LocalStorage` and `S3Storage` implement the 4 primitives
- [ ] The semantic `save_*` methods live in the ingestion layer on top of `paths.py`
- [ ] `ZoneProcessor` still works (Bronze ingestion with no regressions)

**v4 reference:** §3.2, §5.5, §17·D1

---

## #7 — Create `src/common/paths.py` (shared path contract) `(enhancement, architecture)`

**What it involves.** Neutral module that Bronze, Silver, and Gold all import
from to compute Hive-style paths. Couples the stages to a stable third party,
not to each other.

**Acceptance criteria**
- [ ] Functions for Bronze prefixes (`measurements`, `metadata`) by `zone`/`ingest_date`
- [ ] Functions for Silver partitions (`zone`/`event_date`) and Gold
- [ ] Generated paths match the documented layout
- [ ] Pure tests for `paths.py`

**v4 reference:** §5.7, §4.1–§4.3

---

## #8 — Silver layer — transformation (`src/transformation/`) `(enhancement, silver)`

**What it involves.** Create the Silver layer: read all `ingest_date=`
partitions from Bronze, flatten the `page-N.json` files, clean, deduplicate
(key `sensor_id` + `period.datetimeFrom.utc`), and write Parquet partitioned by
`event_date=`. Full rebuild with overwrite by partition (D5). Entry point
`src/transformation/main.py` with CLI `--zone --rebuild --storage`.

**Acceptance criteria**
- [ ] `src/transformation/main.py` with CLI (`--zone --rebuild --storage`)
- [ ] Reads Bronze via the contract's primitives (does not import ingestion code)
- [ ] Flattened to one row per measurement with expected columns/types
- [ ] Global deduplication; idempotent run (full rebuild)
- [ ] Writes Parquet (snappy) to `silver/zone=/event_date=`
- [ ] Runs against the Guadalajara sample data locally

**v4 reference:** §5.6, §4.4, §4.5, §9.3, §10, §17·D5

---

## #9 — Gold layer — modeling (`src/aggregation/`) `(enhancement, gold)`

**What it involves.** Dimensional modeling on top of Silver: `fact_measurements`
plus dimensions (`dim_sensor`, `dim_location`, `dim_parameter`, `dim_date`) and
aggregates by parameter/zone/day/hour. Its own entry point and Dockerfile.

**Acceptance criteria**
- [ ] Grain defined (star schema vs. wide tables)
- [ ] `src/aggregation/main.py` with CLI
- [ ] Aggregations check out against a known Silver (test)
- [ ] Writes Parquet to `gold/`

**v4 reference:** §14 (Phase 4), §11.3

---

## #10 — Dockerize per stage `(enhancement, infra)`

**What it involves.** `requirements/` split per stage, one Dockerfile per stage
with selective `COPY`, `.dockerignore`, `ENTRYPOINT` with passable args.

**Acceptance criteria**
- [ ] `requirements/bronze.txt`, `silver.txt`, `gold.txt`
- [ ] `docker/bronze.Dockerfile` and `docker/silver.Dockerfile`
- [ ] `.dockerignore` excludes `bronze/`, `.venv/`, `.env`, `.git/`, `__pycache__/`
- [ ] The Bronze image contains no Silver code (verifiable with `ls -R`)

**v4 reference:** §6

---

## #11 — CI pipeline (GitHub Actions) `(enhancement, ci)`

**What it involves.** Workflows: tests on every push; `bronze-daily` with the
build-once-push pattern to `ghcr.io`, matrix per zone, `fail-fast: false`,
`max-parallel: 1`; separate `catalog-weekly`. Secrets in GitHub Secrets.

**Acceptance criteria**
- [ ] Test workflow on every push
- [ ] `bronze-daily` with `needs:` (build → extract) and matrix per zone
- [ ] `max-parallel: 1` to protect the rate limit; `workflow_dispatch` for backfills
- [ ] Separate `catalog-weekly`
- [ ] No secrets in a committed `.env`

**v4 reference:** §7.4, §7.6, §7.7, §14 (Phases 2–3)

---

## #12 — Proactive dual-window RateLimiter (D4) `(enhancement, rate-limiting)`

**What it involves.** Replace `sleep_by_rate` (reactive) with a proactive
`RateLimiter` (deque + lock, 55/min and 1900/hour) shared across threads, with
`acquire()` before every request including pagination. Requires rewriting the
fetchers' signature to receive the limiter. Prerequisite for
`ThreadPoolExecutor`.

**Acceptance criteria**
- [ ] Dual-window `RateLimiter` with thread-safe `acquire()` (sleeps outside the lock)
- [ ] `acquire()` before every HTTP request, including pagination
- [ ] Fetchers receive the limiter (updated signature)
- [ ] `ThreadPoolExecutor(max_workers=4)` per zone, per-sensor failure isolated
- [ ] Tests: both windows respected under concurrency

**v4 reference:** §8.0, §8.3, §8.4, §8.5, §17·D4

---

## #13 — Unify metadata writes to "always overwrite" — fix `LocalStorage` (D3) `(bug, storage)`

**What it involves.** Today `LocalStorage` **skips** writing if the metadata
file already exists (`skip-if-exists`), while `S3Storage` always overwrites.
This breaks idempotency and diverges by backend. Fix `LocalStorage` so it
always overwrites, like S3.

**Acceptance criteria**
- [x] `LocalStorage` overwrites metadata even if the file exists
- [x] Identical behavior between local and S3 on same-day re-runs
- [x] Note/test confirming idempotency

**v4 reference:** §9.1, §17·D3

**Done on `dev`.** The three guards in `local_filesystem.py` are gone; the
methods now mirror `s3_storage.py:44-60`. `tests/ingestion/test_local_storage_overwrite.py`
covers all three writers and fails against the pre-fix code. The boolean return
stays as `True` only to match S3 — it is dead code (`zone_processor.py:68`, `:91`,
`:113` discard it) and is removed in **#6**, when these methods move up a layer.

---

## #14 — Remove latent Silver-like code from `ingestion/` (D2) `(refactor, cleanup)`

**What it involves.** Remove from the ingestion layer the `event_date` grouping
code (currently disabled/commented out): `LocalStorage.measurements_event_date_dir()`,
`LocalStorage.save_measurements_by_event_date()`, and
`ZoneProcessor._organize_by_event_date()`. Leaves Bronze 100% pure (satisfies R1).
The logic is recoverable from git (`git show 1062792:…`) as a seed when building Silver.

**Acceptance criteria**
- [ ] The three methods/function and their commented-out calls removed
- [ ] `ingestion/` contains no `event_date` transformation logic
- [ ] Bronze ingestion still works (only `save_measurements_raw`)
- [ ] Git reference noted in the issue/PR (commit `1062792`; introduced in `e278a88`)

**v4 reference:** §3.7, §5.4, §17·D2

---

## #15 — Bug: error message uses `S3_BUCKET_NAME` instead of `AWS_S3_BUCKET_NAME` `(bug, good first issue)`

**What it involves.** In `src/ingestion/openaq/pipeline/orchestrator.py` (~L34-35)
the error message says `S3_BUCKET_NAME`, but the variable actually read (via
`settings.s3_bucket()`) is `AWS_S3_BUCKET_NAME`. It's cosmetic in the text, but
confusing. Align the message with the actual variable name.

**Acceptance criteria**
- [x] The error message names `AWS_S3_BUCKET_NAME`
- [x] The `Add: …` line suggests `AWS_S3_BUCKET_NAME=your-bucket-name`

**v4 reference:** §3.3 (code note)

**Done on `dev`** in commit `0c00e34`, which covers **both** occurrences:
`orchestrator.py` and the `argument_parser.py` help text. First change to `src/`
since November 2025.

---

## #17 — Structured logging and visible swallowed errors `(enhancement, infra)`

> Added on 2026-09-06, after Phase 0 was drafted. Found while resolving #13.

**What it involves.** There is no `logging` anywhere in `src/`: 82 `print()`
calls across six files (`output_formatter.py` 36, `zone_processor.py` 25,
`orchestrator.py` 11, `config_loader.py` 6, `main.py` 2, `http_client.py` 2).
With `print()` there are no severity levels, no timestamps, everything is mixed
into stdout, and there is no way to route output to a file or CloudWatch without
capturing the whole stream. That is the difference between being able to
diagnose a failed unattended run and not.

The concrete trigger is worse than cosmetic. `ZoneProcessor._process_sensors`
catches per-location exceptions, prints one line among forty and `continue`s
(`zone_processor.py:108-110`) **without incrementing `zone_stats['errors']`**.
A run where the API failed on three locations still writes an incomplete
`sensors_index.json` and reports `Zone ... completed successfully`
(`zone_processor.py:53`). The run lies about its own outcome.

This blocks real production use, and it blocks **#11** (CI): a scheduled
workflow whose only signal is unstructured stdout cannot be monitored.

**Acceptance criteria**
- [ ] A single logging setup (level configurable via env var), no `print()` left
      on execution paths — the CLI's user-facing output in `output_formatter.py`
      may stay as `print`, since it *is* the program's output, not diagnostics
- [ ] Swallowed exceptions logged at `warning`/`error` **and** counted in
      `zone_stats['errors']`
- [ ] The end-of-run summary reports failed locations/sensors, so an incomplete
      run cannot report success
- [ ] Log lines carry timestamp, level and zone

**Note.** Independent of #6 and #14; it can land before or after them. It is a
prerequisite for #11 being useful.

---

## Labels

All nine already exist in the repo, alongside GitHub's defaults.

| Label | Description |
|---|---|
| `architecture` | Design decisions about system structure and contracts |
| `silver` | Related to the Silver layer (transformation) |
| `gold` | Related to the Gold layer (aggregation) |
| `infra` | Infrastructure: Docker, deployment, environment |
| `ci` | Continuous integration workflows |
| `rate-limiting` | Related to API rate limiting and throttling |
| `storage` | Related to StorageInterface and its backends |
| `refactor` | Restructuring existing code without changing behavior |
| `cleanup` | Code removal or simplification, no new behavior |

`bug`, `enhancement`, and `good first issue` ship with GitHub by default.

---

## Open pull requests

| PR | Closes | Note |
|---|---|---|
| [#16](https://github.com/MemoOrtiz/Air-Quality-ETL/pull/16) | #15 | External contribution (fork `slegarraga`). Targets `main` directly instead of `dev`, and fixes only `orchestrator.py`. **Superseded:** commit `0c00e34` on `dev` fixes both occurrences, including the `argument_parser.py:93` help text the PR leaves untouched. To be closed with thanks. |
