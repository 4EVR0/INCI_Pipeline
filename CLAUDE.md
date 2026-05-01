# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Running the Pipeline (Docker)

```bash
# Build images
docker compose build

# Run each stage (Bronze can run in parallel)
docker compose run --rm bronze-kcia
docker compose run --rm bronze-cosing
docker compose run --rm silver-mapping
docker compose run --rm gold-pipeline

# Run individual pipeline scripts directly (requires .env)
python -m pipeline.kcia_pipeline.app
python -m pipeline.cosing_pipeline.app
python -m pipeline.silver_mapping.kcia_cosing.run_mapping
python -m pipeline.gold_pipeline.kcia_cosing.run_gold
python -m pipeline.gold_pipeline.kcia_cosing.run_gold --no-upload  # skip S3
```

### Airflow

```bash
# First-time setup
docker compose -f docker-compose.airflow.yml up airflow-init --build

# Start Airflow (UI at http://localhost:8080, admin/admin)
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

## Architecture

Medallion Architecture (Bronze → Silver → Gold) for cosmetic ingredient data from two sources:

- **KCIA** (대한화장품협회): Korean ingredient data via HTML crawling (~21,800 rows)
- **CosIng** (EU): Global ingredient data via REST API with recursive query splitting (~119,000 rows)

### DAG flow

```
bronze_kcia ──┐
              ├──► silver_mapping ──► gold_pipeline
bronze_cosing─┘
```

Monthly schedule: `0 1 1 * *` via `dags/inci_monthly_pipeline.py`

### Pipeline Stage Structure

Each stage follows a consistent pattern:

```
config.py      — loads Settings dataclass from env vars (dotenv)
extract.py     — data fetching (HTTP/API)
transform.py   — normalization and cleaning
validate.py    — row count checks and schema validation
load_s3.py     — boto3 upload to S3
app.py         — orchestrates extract → transform → validate → save → upload
```

### Silver Mapping (`pipeline/silver_mapping/kcia_cosing/`)

Matches KCIA ↔ CosIng ingredients in priority order:
1. Exact matches: `exact_cas`, `exact_basic`, `exact_full_normalized`, `exact_paren_removed`, `exact_word_sorted`
2. Fuzzy match ≥ `FUZZY_AUTO_THRESHOLD` (default 95) → auto-confirmed
3. Fuzzy match ≥ `FUZZY_REVIEW_THRESHOLD` (default 90) → review queue
4. CAS set overlap → resolves name variants

Outputs: `matched_final`, `fuzzy_review`, `final_unmatched`, `graphrag_map`

Input mode is controlled by `MAPPING_INPUT_MODE=bronze_local|s3`.

### CosIng Query Splitting

CosIng API caps results at ~10,000 per query. `pipeline/cosing_pipeline/extract/splitter.py` recursively splits by prefix (`p*` → `pa*`, `pb*`, …) when `SAFE_LIMIT` is exceeded.

### Checkpointing / Resume

Both Bronze pipelines write checkpoint state to disk during extraction. On interruption, re-running resumes from the checkpoint. Checkpoint files are deleted on successful completion (`KCIA_CLEAR_CHECKPOINT_ON_SUCCESS`, `clear_checkpoint_on_success`).

### S3 Layout

```
{S3_BUCKET}/{prefix}/batch=YYYY-MM/  ← Bronze
{S3_SILVER_PREFIX}/kcia_cosing/batch=YYYY-MM/  ← Silver
{S3_GOLD_PREFIX}/kcia_cosing/batch=YYYY-MM/    ← Gold
```

### common/

- `metadata.py` — shared `build_*_metadata()` helpers and `write_json()`
- `paths.py` — `get_kcia_bronze_paths()` and `ensure_dir()` utilities

## Environment Variables

Create `.env` at project root. Key variables:

| Variable | Description |
|---|---|
| `S3_BUCKET` | Target S3 bucket |
| `KCIA_BASE_URL` | KCIA HTML crawl URL |
| `COSING_API_KEY` | CosIng REST API key |
| `BATCH_MONTH` | Override batch month (YYYY-MM); defaults to current month |
| `MAPPING_INPUT_MODE` | `bronze_local` (default) or `s3` |
| `FUZZY_AUTO_THRESHOLD` | Fuzzy score for auto-confirm (default 95) |
| `FUZZY_REVIEW_THRESHOLD` | Fuzzy score for review queue (default 90); must be < auto threshold |
| `SAVE_INTERMEDIATE` | Save intermediate mapping files (default true) |
| `HOST_PROJECT_DIR` | Absolute host path for DockerOperator volume mounts (Airflow only) |
