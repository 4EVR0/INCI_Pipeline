# INCI Data Pipeline (KCIA + CosIng)

## Overview

화장품 성분 데이터(KCIA, CosIng)를 수집하고 **Medallion Architecture(Bronze → Silver → Gold)** 구조로 적재·정제하는 ETL 파이프라인입니다.

- **KCIA (대한화장품협회)** — 국내 성분 데이터 (HTML 크롤링)
- **CosIng (EU)** — 글로벌 성분 데이터 (REST API)
- 실행 주기: 월 1회 (Airflow DAG 자동화)
- 목적: 국내/국제 성분 통합 데이터셋 구축 및 Graph-RAG 시스템 활용

---

## Architecture

```
[ KCIA Website ]        [ CosIng API ]
        |                      |
     Extract                Extract
        |                      |
     Transform          Query Splitting
        |                      |
     Validate           Raw Collection
        |                      |
        +---------> Bronze Layer <--------+
                         |
                   Silver Mapping
                         |
       [ matched_final / fuzzy_review / unmatched ]
                         |
                    Gold Layer
                         |
              Graph-RAG Ingredients Dataset
```

---

## Medallion Architecture

### Bronze Layer

Raw 데이터를 최소 전처리만 거쳐 저장합니다.

| Source | 설명 |
|---|---|
| KCIA | HTML 크롤링, 중복 제거된 정형 데이터 (~21,800행) |
| CosIng | REST API, query splitting으로 전체 수집 (~119,000행) |

### Silver Layer

- CosIng deduplication (`key_cas`, `key_basic`, `key_full` 기준)
- KCIA ↔ CosIng 성분 매핑 (exact match + fuzzy match + CAS overlap)
- 이름 정규화 및 CAS 검증
- 결과: `matched_final` / `fuzzy_review` / `final_unmatched` / `graphrag_map`
- 자동 매핑률: **90.90%**

### Gold Layer

- Silver matched_final → 분석·서빙용 최종 성분 데이터셋
- Graph-RAG 검색 시스템 활용

---

## Project Structure

```
INCI_data/
├── pipeline/                          # 파이프라인 소스코드
│   ├── kcia_pipeline/                 # KCIA Bronze ETL
│   │   ├── app.py
│   │   ├── config.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── validate.py
│   │   ├── parser.py
│   │   ├── http_client.py
│   │   ├── load_s3.py
│   │   └── models.py
│   ├── cosing_pipeline/               # CosIng Bronze ETL
│   │   ├── app.py
│   │   ├── config.py
│   │   ├── load_s3.py
│   │   ├── models.py
│   │   ├── validate.py
│   │   ├── extract/
│   │   │   ├── client.py
│   │   │   ├── extract.py
│   │   │   └── splitter.py
│   │   └── transform/
│   │       ├── parser.py
│   │       └── transform.py
│   ├── silver_mapping/                # Silver 매핑 파이프라인
│   │   └── kcia_cosing/
│   │       ├── config.py
│   │       ├── io.py
│   │       ├── matcher.py
│   │       ├── normalizer.py
│   │       ├── pipeline.py
│   │       ├── run_mapping.py
│   │       └── s3_io.py
│   └── gold_pipeline/                 # Gold 파이프라인
│       └── kcia_cosing/
│           ├── config.py
│           ├── run_gold.py
│           └── transform.py
│
├── common/                            # 공통 유틸리티
│   ├── metadata.py
│   └── paths.py
│
├── dags/                              # Airflow DAG
│   └── inci_monthly_pipeline.py
│
├── data/                              # 로컬 데이터 (gitignored)
│   ├── bronze/
│   │   ├── kcia/batch=YYYY-MM/
│   │   └── cosing/batch=YYYY-MM/
│   ├── silver/
│   │   └── kcia_cosing/batch=YYYY-MM/
│   └── gold/
│       └── batch=YYYY-MM/
│
├── logs/
├── Dockerfile                         # 파이프라인 이미지
├── Dockerfile.airflow                 # Airflow 이미지
├── docker-compose.yml                 # 파이프라인 수동 실행용
├── docker-compose.airflow.yml         # Airflow 실행용
└── requirements.txt
```

---

## Quick Start

### 사전 조건

- Docker / Docker Compose
- `.env` 파일 설정 (아래 환경변수 섹션 참고)

### 파이프라인 수동 실행 (docker-compose.yml)

```bash
# 이미지 빌드
docker compose build

# Bronze (병렬 실행 가능)
docker compose run --rm bronze-kcia
docker compose run --rm bronze-cosing

# Silver
docker compose run --rm silver-mapping

# Gold
docker compose run --rm gold-pipeline
```

### Airflow 자동화 실행 (docker-compose.airflow.yml)

매월 1일 01:00에 자동 실행됩니다.

```bash
# 최초 1회: 이미지 빌드 + DB 초기화
docker compose -f docker-compose.airflow.yml up airflow-init --build

# Airflow 시작
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

웹 UI: `http://localhost:8080` (admin / admin)
→ `inci_monthly_pipeline` DAG를 Unpause하면 자동 스케줄 시작

---

## Environment Variables

`.env` 파일을 프로젝트 루트에 생성합니다.

```bash
# 공통
HOST_PROJECT_DIR=/path/to/INCI_data   # DockerOperator 볼륨 마운트용 (Airflow)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=ap-northeast-2
S3_BUCKET=your-s3-bucket

# KCIA
KCIA_BASE_URL=https://kcia.or.kr/cid/search/ingd_list.php
KCIA_S3_PREFIX=INCI_data/kcia

# CosIng
COSING_API_KEY=your-cosing-api-key
COSING_S3_PREFIX=INCI_data/cosing

# Silver
MAPPING_INPUT_MODE=bronze_local       # bronze_local | s3
S3_SILVER_PREFIX=INCI_data_silver/

# Gold
S3_GOLD_PREFIX=INCI_data_gold/

# 매핑 옵션
FUZZY_AUTO_THRESHOLD=95
FUZZY_REVIEW_THRESHOLD=90
SAVE_INTERMEDIATE=true
```

> `BATCH_MONTH`는 설정하지 않으면 실행 시점의 연월로 자동 결정됩니다.

---

## Key Features

### 1. Query Splitting (CosIng)

CosIng API는 쿼리당 최대 ~10,000행 제한이 있습니다. prefix 기반 재귀 분할로 전체 데이터를 수집합니다.

```
p* → pa*, pb*, pc* ...  (SAFE_LIMIT 초과 시 재귀 분할)
```

### 2. Resume / Checkpoint

크롤링 중단 시 체크포인트에서 재개합니다. 완료 후 체크포인트 파일은 자동 삭제됩니다.

### 3. Silver Ingredient Mapping

| 매칭 종류 | 방법 |
|---|---|
| Exact | `exact_cas`, `exact_basic`, `exact_full_normalized`, `exact_paren_removed`, `exact_word_sorted` |
| Fuzzy | `fuzzy_auto` (≥95점 자동 확정), `fuzzy_review` (≥90점 검토 대상) |
| CAS Overlap | CAS set 교집합 비교로 표기 차이 해소 |

CAS overlap 도입으로 자동 매핑률이 **81.66% → 90.90%** 향상되었습니다.

### 4. Airflow DAG (월간 자동화)

```
bronze_kcia ──┐
              ├──► silver_mapping ──► gold_pipeline
bronze_cosing─┘
```

스케줄: `0 1 1 * *` (매월 1일 01:00)

---

## Data Characteristics (2026-04 기준)

| 레이어 | 파일 | 행 수 |
|---|---|---|
| Bronze | kcia_bronze.csv | 21,805 |
| Bronze | cosing_bronze.csv | 119,361 |
| Silver | kcia_cosing_matched_final.csv | 19,892 |
| Silver | kcia_cosing_fuzzy_review_latest.csv | 515 |
| Silver | kcia_cosing_unmatched_final.csv | 1,475 |
| Gold | kcia_cosing_gold_ingredients.csv | 18,714 |
