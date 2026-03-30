# 📦 CosIng ETL Pipeline (Bronze Layer)

## 📌 Overview

CosIng Pipeline은 유럽연합(EU)의 화장품 성분 데이터베이스인 **CosIng**으로부터 성분 데이터를 수집하고,

최소한의 전처리를 거쳐 **S3 Bronze Layer에 저장하는 ETL 파이프라인**

---

## 🏗 Architecture

```
CosIng API
   ↓
[Extract]
   ↓
[Query Splitting]
   ↓
[Raw Data Collection]
   ↓
[Transform (Minimal)]
   ↓
[S3 Upload - Bronze]
```

---

## ⚙️ Key Features

### 1. Query Splitting (핵심)

CosIng API는 한 query당 최대 약 10,000 rows 제한이 존재

이를 해결하기 위해 prefix 기반 query splitting을 적용

예시:

```
p* → pa*, pb*, pc* ...
```

- 각 query의 결과 개수를 사전 조회
- SAFE_LIMIT (예: 9500) 초과 시 재귀적으로 분할
- 모든 query가 제한 이하가 될 때까지 반복

👉 이를 통해 **데이터 유실 없이 전체 성분 수집 가능**

---

### 2. Count Cache

- 동일 query에 대한 count 요청 최소화
- `count_cache.json` 파일에 결과 저장
- API 호출 수 감소 및 속도 향상

---

### 3. Bronze Layer 설계

Bronze Layer는 다음 원칙을 따른다:

- 원본 데이터 최대한 보존
- 최소한의 정제만 수행
- 중복 허용 (Query overlap 가능)

👉 따라서 Bronze 데이터는 **완전 정제된 데이터가 아님**

---

### 4. S3 업로드

- 수집된 데이터를 CSV 형태로 변환
- 날짜 기준으로 S3에 저장

예시:

```
s3://bucket/bronze/cosing/2026-03-24/cosing_bronze.csv
```

---

## 📁 Project Structure

```
cosing_pipeline/
│
├── app.py                  # 파이프라인 실행 entrypoint
├── config.py               # 환경변수 및 설정 관리
├── logging_utils.py        # 로깅 설정
│
├── extract/
│   ├── extract.py          # 전체 수집 로직
│   ├── client.py           # CosIng API 호출
│   ├── splitter.py         # query splitting 로직
│   ├── parser.py           # API 응답 파싱
│   └── collector.py        # 페이지 단위 데이터 수집
│
├── transform/
│   └── transform.py        # 최소 전처리
│
│-── s3_loader.py           # S3 업로드
│
└── utils/
    └── utils.py            # 공통 유틸
```

---

## 🔑 Environment Variables (.env)

### 필수 값

```
S3_BUCKET=your-bucket
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
COSING_API_KEY=your-api-key
```

---

### 선택 값 (기본값 존재)

```
COSING_PAGE_SIZE=100
COSING_TIMEOUT=30
COSING_SLEEP_SEC=0.1
COSING_SAFE_LIMIT=9500
COSING_MAX_DEPTH=5
COSING_MAX_SEEDS=36
```

---

## ▶️ How to Run

```
cd INCI_data
python-m cosing_pipeline.app
```

---

## 📊 Data Characteristics

- Bronze 데이터는 중복을 포함할 수 있음
- query overlap으로 인해 row 수가 증가할 수 있음
- 실제 고유 성분 수는 약 25,000개 수준

👉 이후 Silver Layer에서 deduplication 수행

---

## 🧠 Summary

- CosIng API의 row limit 문제를 해결하기 위해 query splitting 적용
- Bronze Layer는 raw 데이터 보존에 집중
- 중복 및 정제는 이후 단계(Silver)에서 처리