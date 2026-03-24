# 📦 INCI Data Pipeline (KCIA + CosIng)

## 📌 Overview

본 파이프라인은 화장품 성분 데이터(KCIA, CosIng)를 수집하고,

이를 **Medallion Architecture 기반 데이터 레이크(S3)** 에 적재하는 ETL 파이프라인입니다.

- 데이터 소스:
    - **KCIA (대한화장품협회)** – 국내 성분 데이터
    - **CosIng (EU)** – 글로벌 성분 데이터
- 실행 주기: 월 1회 (준정적 reference 데이터)
- 목적:
    - 성분 통합 데이터셋 구축
    - 이후 **매핑 / Graph / RAG 시스템** 활용

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
        └──────→ [ Bronze Layer (S3) ] ←──────┘
                           ↓
                    (Future)
                        ↓
                  Silver (Dedup / Mapping)
                        ↓
                  Gold (Analytics / Graph)
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

### ⚪ Silver Layer (Planned)

- CosIng deduplication (`substance_id` 기준)
- KCIA ↔ CosIng 매핑
- 데이터 정규화

---

### 🟡 Gold Layer (Planned)

- 분석용 데이터셋
- Graph 기반 구조
- RAG / 검색 시스템 활용

---

## ⚙️ Key Features

### 1. Multi-source ETL Pipeline

- 서로 다른 구조의 데이터 소스 처리:
    - HTML 크롤링 (KCIA)
    - REST API (CosIng)

---

### 2. Query Splitting (CosIng 핵심)

CosIng API는 최대 약 10,000 rows 제한 존재

```
p* → pa*, pb*, pc* ...
```

- prefix 기반 분할
- SAFE_LIMIT 이하로 재귀 분할
- 데이터 유실 방지

---

### 3. Data Validation (KCIA)

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

### 5. S3 Data Lake 적재

```
s3://{bucket}/bronze/
    ├── kcia/
    │   └── ingest_date=YYYY-MM-DD/
    │       └── kcia.csv
    │
    └── cosing/
        └── YYYY-MM-DD/
            └── cosing_bronze.csv
```

---

## 📁 Project Structure

```
INCI_data/
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
│   ├── parser.py
│
├── cosing_pipeline/
│   ├── app.py
│   ├── config.py
│   ├── logging_utils.py
│   │
│   ├── extract/
│   │   ├── extract.py
│   │   ├── client.py
│   │   ├── splitter.py
│   │   ├── parser.py
│   │   └── collector.py
│   │
│   ├── transform/
│   │   └── transform.py
│   │
│   ├── s3_loader.py
│   │
│   └── utils/
│       └── utils.py
│
└── utils/
    └── logging_utils.py
```

---

## 🔑 Environment Variables

```
# 공통
S3_BUCKET=your-bucket
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret

# KCIA
KCIA_BASE_URL=https://kcia.or.kr/cid/search/ingd_list.php

# CosIng
COSING_API_KEY=your-api-key

# 옵션
COSING_SAFE_LIMIT=9500
COSING_PAGE_SIZE=100
REQUEST_SLEEP=0.3
```

---

## ▶️ How to Run

```
# KCIA
python-m kcia_pipeline.app

# CosIng
python-m cosing_pipeline.app
```

---

## 📊 Data Characteristics

### KCIA

- 약 21,000+ rows
- 중복 제거된 정형 데이터

### CosIng

- 약 30,000+ unique substances
- Bronze 기준 중복 포함 가능 (query overlap)
