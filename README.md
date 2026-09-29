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

### 식약처 국내 규제 필터 (월간 DAG `mfds_regulation`)

식약처 **화장품 사용제한 원료정보**(`CsmtcsUseRstrcInfoService`)의 한국 행으로 성분별
국내 규제 상태를 만든다. 추천 서버는 `banned`를 후보에서 제외하고 `restricted`는 한도 문구를 보여 준다.

| kr_reg_status | 기준 | 예 | 추천 |
|---|---|---|---|
| `banned` | 금지 고시, 단서조항·배합한도·조건 문구 없음 | 아젤라익애씨드, 하이드로퀴논 | 제외 |
| `conditional` | 금지지만 원료 품질 조건(단서조항, "초과하는", "다만" 등) | 탤크(석면), 리모넨(과산화물가), 코카마이드DEA | 정상 |
| `restricted` | 한도 또는 배합한도 | 살리실릭애씨드, 페녹시에탄올 1%, 토코페롤 20% | 추천 + `kr_limit_note` |

- `REGULATE_TYPE`은 국가 공통 성분 단위 라벨이라 국내 판정에 쓰지 않는다. 금지로 판정됐지만
  규제 원료정보(`CsmtcsReglMaterialInfoService`)에서 한국이 `LIMIT_NATIONAL`에도 있으면
  (녹색3호, 카본블랙) `restricted` + 확인 필요 문구로 낮추고 review로 보낸다.
- 매칭은 이름 정확 일치(소문자·영숫자·한글만)만 자동 적용한다. "그 염류 및 유도체"는 모물질명만
  남겨 비교하고 다른 성분명으로 확장하지 않는다(포타슘아젤로일다이글리시네이트는 금지 아님).
  `banned`는 INCI명 일치 또는 KCIA명+CAS 일치일 때만 적용한다(Gold의 KCIA명 오매핑 방지).
  CAS·이명 일치는 `kr_regulation_review.csv`로 간다.
- KCIA Gold에 없는 금지 성분(예: AZELAIC ACID)을 위해 GraphRAG `target_ingredients.csv`도
  함께 매칭한다(`--targets` 또는 `TARGET_INGREDIENTS_CSV`).

```bash
# .env: REGULATION_API_KEY (data.go.kr 인코딩/디코딩 키 모두 가능)
python -m pipeline.mfds_regulation.run                    # 수집 → Silver → Gold
python -m pipeline.mfds_regulation.run \
  --raw rstrc.json --raw-regl regl.json --gold gold.csv  # 수집 생략
```

출력: `data/{bronze,silver,gold}/mfds_regulation/run_id=<id>/`, Neo4j 적재 입력은
`gold/.../ingredient_kr_regulation.csv`(`inci_name, kr_reg_status, kr_limit_note, ...`).

### 식약처 성분 API 파일럿 (선택 실행)

[식약처 화장품 원료성분정보 API](https://www.data.go.kr/data/15111774/openapi.do)는
표준 한글·영문명, CAS 번호, 기원·정의, 이명을 제공한다. 이는 **성분 식별 자료**이지
효능·제품 전성분·무향 여부의 근거가 아니다. 파일럿은 월간 DAG 밖에서 실행하며
Gold, Neo4j, 추천 점수를 바꾸지 않는다.

API 활용신청 후 발급받은 인코딩 키 또는 디코딩 키를 gitignored `.env`에
`MFDS_API_KEY`로 설정한다. 수집기가 쿼리 인코딩을 처리한다. 키를 커밋하거나
명령행 인자로 넘기지 않는다. 우선 한 페이지와 실제
JSON 구조를 확인한다:

```bash
python -m pipeline.mfds_pipeline.pilot \
  --max-pages 1 --out-dir dev_data/mfds-pilot
```

기존 Gold CSV와 커버리지를 비교하려면 식약처 **전체** 스냅샷을 수집하고
`--gold /path/to/kcia_cosing_gold_ingredients.csv`를 지정한다. `metadata.json`의
`complete`가 `true`가 될 때까지 `--max-pages`를 늘려야 한다. 불완전한
스냅샷으로는 커버리지를 계산하지 않는다. 감사 결과는 이름+CAS 일치,
두 이름 일치·한 이름 일치, 한글/영문명 불일치, CAS 충돌, 중복 후보, 미매칭을
분리한다. 운영 적재 전 이 결과를
검토해야 한다. API의 확인된 `--page-size` 상한은 500이다. 원본 파일은
gitignored `dev_data/`에 둔다.

수집 후 매칭 기준을 바꿔 다시 감사할 때는 `--snapshot-dir`로 기존 결과를
읽을 수 있다. 이 모드에서는 API를 다시 호출하지 않는다.

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
│   ├── mfds_regulation/               # 식약처 국내 규제 필터 (collect, transform, run)
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

# 식약처 규제 필터
REGULATION_API_KEY=your-data-go-kr-service-key
TARGET_INGREDIENTS_CSV=/app/config/target_ingredients.csv   # 선택

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
              ├──► silver_mapping ──► gold_pipeline ──► mfds_regulation
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
