# 📦 INCI Data Pipeline (KCIA + CosIng)

## 📌 Overview

본 파이프라인은 화장품 성분 데이터(KCIA, CosIng)를 수집하고,

이를 **Medallion Architecture 기반 데이터 레이크(Bronze → Silver)** 구조로 적재 및 정제하는 ETL 파이프라인입니다.

- 데이터 소스:
    - **KCIA (대한화장품협회)** – 국내 성분 데이터
    - **CosIng (EU)** – 글로벌 성분 데이터
- 실행 주기: 월 1회 (준정적 reference 데이터)
- 목적:
    - 국내/국제 성분 통합 데이터셋 구축
    - KCIA ↔ CosIng 성분 매핑
    - 이후 **Graph / RAG 시스템** 활용

---

## 🏗️ Architecture

```
[ KCIA Website ]        [ CosIng API ]
        ↓                      ↓
     Extract                Extract
        ↓                      ↓
     Transform            Query Splitting
        ↓                      ↓
     Validate             Raw Collection
        ↓                      ↓
        └──────→ [ Bronze Layer ] ←──────┘
                           ↓
                     Silver Mapping
                           ↓
         [ KCIA ↔ CosIng Matched / Review / Unmatched ]
                           ↓
                  Graph-RAG Mapping Dataset
```

---

## 🧱 Medallion Architecture

### 🟤 Bronze Layer

- Raw 데이터 저장
- 최소 전처리만 수행
- 데이터 원본 최대 보존

| Source | 특징 |
| --- | --- |
| KCIA | 중복 제거 포함된 정형 데이터 |
| CosIng | query overlap으로 중복 포함 가능 |

---

### ⚪ Silver Layer

- CosIng deduplication (`key_cas`, `key_basic`, `key_full` 등 기준)
- KCIA ↔ CosIng 성분 매핑
- 이름 정규화 및 CAS 검증
- 결과 분기:
    - `matched_final`
    - `fuzzy_review`
    - `final_unmatched`
- Graph-RAG용 성분 매핑 테이블 생성

---

### 🟡 Gold Layer (Planned)

- 분석용 데이터셋
- Graph 기반 구조
- RAG / 검색 시스템 활용

---
## ▶️ How to Run

### 0. Prerequisites

- Python 3.12+
- Docker / Docker Compose
- `.env` file configured
- dependencies installed from `requirements.txt`

```bash
pip install -r requirements.txt
```

---
### 🔐 환경 설정

이 프로젝트는 실행에 필요한 설정값을 `.env` 파일로 관리합니다.

프로젝트를 실행하기 전에 먼저 예시 파일을 복사하여 `.env` 파일을 생성하세요.

```bash
cp .env.example .env
```
---

#### 🔐 설정 1. 공통 AWS 인증 키 설정

```bash
# 공통
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_DEFAULT_REGION=ap-northeast-2
S3_BUCKET=your-s3-bucket
```

---

#### 🔐 설정 2. 배치 기준 날짜 설정

```bash
# 배치 기준 (필요 시, 직접 수정)
BATCH_MONTH=YYYY-MM
INGEST_DATE=YYYY-MM-DD

# Silver 매핑용 로컬 입력 경로 (직접 입력)
KCIA_LOCAL_PATH=bronze/kcia/batch=YYYY-MM/kcia_bronze.csv
COSING_LOCAL_PATH=bronze/cosing/batch=YYYY-MM/cosing_bronze.csv
```

(미설정 시, 시스템 시간으로 자동 설정됩니다.)

---

#### 🔐 설정 3. CosIng API key

```bash
COSING_API_KEY=your-cosing-api-key
```

---

### 1. Local 실행

### 1-1. KCIA Bronze

```bash
python-m kcia_pipeline.app
```

Outputs:

- `bronze/kcia/batch=YYYY-MM/kcia_bronze.csv`
- `bronze/kcia/batch=YYYY-MM/metadata.json`

### 1-2. CosIng Bronze

```bash
python-m cosing_pipeline.app
```

Outputs:

- `bronze/cosing/batch=YYYY-MM/cosing_bronze.csv`
- `bronze/cosing/batch=YYYY-MM/cosing_bronze.parquet`
- `bronze/cosing/batch=YYYY-MM/metadata.json`

### 1-3. KCIA ↔ CosIng Silver Mapping

```bash
python-m silver_mapping.kcia_cosing.run_mapping
```

Outputs:

- `silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_matched_final.csv`
- `silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_fuzzy_review_latest.csv`
- `silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_unmatched_final.csv`
- `silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_graphrag_map.csv`
- `silver/kcia_cosing/batch=YYYY-MM/mapping_summary.csv`

---

### 2. Docker Execution

#### Build

```bash
docker compose build
```

### 2-1. KCIA Bronze

```bash
docker compose run--rm bronze-kcia
```

### 2-2. CosIng Bronze

```bash
docker compose run--rm bronze-cosing
```

### 2-3. KCIA ↔ CosIng Silver Mapping

```bash
docker compose run--rm silver-mapping
```

---

### 3. 추천 실행 순서

### Local

```bash
python-m kcia_pipeline.app
python-m cosing_pipeline.app
python-m silver_mapping.kcia_cosing.run_mapping
```

### Docker

```bash
docker compose build
docker compose run--rm bronze-kcia
docker compose run--rm bronze-cosing
docker compose run--rm silver-mapping
```

---

## ⚙️ Key Features

### 1. Multi-source ETL Pipeline

- 서로 다른 구조의 데이터 소스 처리:
    - HTML 크롤링 (KCIA)
    - REST API (CosIng)

---

### 2. Query Splitting (CosIng 핵심)

CosIng API는 한 query당 최대 약 10,000 rows 제한이 존재한다.

이를 해결하기 위해 prefix 기반 query splitting을 적용하였다.

```
p* → pa*, pb*, pc* ...
```

- prefix 기반 분할
- SAFE_LIMIT 이하로 재귀 분할
- 데이터 유실 방지

---

### 3. Bronze Data Validation

- row 수 검증
- 페이지 수 검증
- strict mode 지원

👉 안정적인 배치 파이프라인 보장

---

### 4. Count Cache (CosIng)

- query별 count 결과 캐싱
- API 호출 수 감소
- 성능 최적화

---

### 5. Silver Ingredient Mapping

KCIA와 CosIng 데이터를 영문명, 정규화 키, CAS 정보를 기준으로 매핑한다.

- exact match:
    - `exact_cas`
    - `exact_basic`
    - `exact_full_normalized`
    - `exact_paren_removed`
    - `exact_word_sorted`
    - `exact_word_sorted_strict`
- fuzzy match:
    - `fuzzy_auto`
    - `fuzzy_review_threshold`

---

### 6. CAS Overlap 기반 매핑 개선

초기 Silver 매핑에서는 이름이 일치하더라도 CAS 문자열 표기 차이 때문에 review로 분류되는 사례가 다수 존재하였다.

이를 개선하기 위해 CAS를 단일 문자열이 아니라 **CAS set으로 분해하여 교집합(overlap)을 비교하는 로직**을 추가하였다.

- 적용 범위:
    - `exact_basic`
    - `exact_full_normalized`
- 결과:
    - 기존 후처리 없이 파이프라인 내부에서 바로 반영
    - 자동 매핑률 **81.66% → 90.90%** 개선

---

### 7. Silver Output Standardization

최종 Silver 결과는 다음 파일로 표준화하여 저장된다.

- `kcia_cosing_matched_final.csv`
- `kcia_cosing_fuzzy_review_latest.csv`
- `kcia_cosing_unmatched_final.csv`
- `kcia_cosing_graphrag_map.csv`
- `mapping_summary.csv`

---

## 📁 Project Structure

```
INCI_data/
│
├── bronze/
│   ├── kcia/
│   │   └── batch=YYYY-MM/
│   │       └── kcia_bronze.csv
│   └── cosing/
│       └── batch=YYYY-MM/
│           └── cosing_bronze.csv
│
├── common/
│   ├── metadata.py
│   └── paths.py
│
├── kcia_pipeline/
│   ├── app.py
│   ├── config.py
│   ├── models.py
│   ├── extract.py
│   ├── transform.py
│   ├── validate.py
│   ├── load_s3.py
│   ├── http_client.py
│   └── parser.py
│
├── cosing_pipeline/
│   ├── app.py
│   ├── config.py
│   ├── load_s3.py
│   ├── models.py
│   ├── validate.py
│   ├── extract/
│   │   ├── client.py
│   │   ├── extract.py
│   │   └── splitter.py
│   ├── transform/
│   │   ├── parser.py
│   │   └── transform.py
│   └── utils/
│       └── logging_utils.py
│
├── silver/
│   └── kcia_cosing/
│       └── batch=YYYY-MM/
│           ├── kcia_cosing_matched_final.csv
│           ├── kcia_cosing_fuzzy_review_latest.csv
│           ├── kcia_cosing_unmatched_final.csv
│           ├── kcia_cosing_graphrag_map.csv
│           └── mapping_summary.csv
│
├── silver_mapping/
│   └── kcia_cosing/
│       ├── config.py
│       ├── io.py
│       ├── matcher.py
│       ├── normalizer.py
│       ├── pipeline.py
│       └── run_mapping.py
│
├── README.md
└── requirements.txt
```

---

## 🔑 Environment Variables

```
# 공통
S3_BUCKET=your-bucket
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=ap-northeast-2

# 배치 기준
BATCH_MONTH=2026-03

# KCIA / CosIng Bronze input
MAPPING_INPUT_MODE=bronze_local
KCIA_LOCAL_PATH=/path/to/bronze/kcia_bronze.csv
COSING_LOCAL_PATH=/path/to/bronze/cosing_bronze.csv

# KCIA
KCIA_BASE_URL=https://kcia.or.kr/cid/search/ingd_list.php

# CosIng
COSING_API_KEY=your-api-key

# 매핑 옵션
FUZZY_AUTO_THRESHOLD=95
FUZZY_REVIEW_THRESHOLD=90
SAVE_INTERMEDIATE=true
```

---

## ▶️ How to Run

```
# 1. KCIA Bronze
python-m kcia_pipeline.app

# 2. CosIng Bronze
python-m cosing_pipeline.app

# 3. KCIA ↔ CosIng Silver Mapping
python-m silver_mapping.kcia_cosing.run_mapping
```

---

## 📊 Data Characteristics

### KCIA

- 약 21,000+ rows
- 중복 제거된 정형 데이터
- 국내 화장품 성분 기준 데이터

### CosIng

- 약 30,000+ raw rows
- Bronze 기준 중복 포함 가능
- query splitting 기반 전체 수집

### Silver Mapping Result

- KCIA 전체 성분: **21,796개**
- 자동 매핑: **19,812개**
- review: **514개**
- unmatched: **1,470개**
- 자동 매핑률: **90.90%**
- 데이터 유실: **0건**

---

## 🧠 Summary

- KCIA / CosIng 원천 데이터를 Bronze Layer에 수집 및 저장
- Silver Layer에서 이름 정규화, exact/fuzzy 매칭, CAS 검증 수행
- CAS overlap 기반 개선 로직을 파이프라인에 통합하여 후처리 없이 최종 매핑률 90.90% 달성
- 최종적으로 Graph-RAG 확장을 위한 성분 통합 매핑 데이터셋을 구축