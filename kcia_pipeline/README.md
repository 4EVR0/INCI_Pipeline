# KCIA Ingredient ETL Pipeline

## 📌 Overview

이 파이프라인은 KCIA(대한화장품협회) 공식 홈페이지에서 성분 데이터를 수집하여

최소 전처리 후 **S3 Bronze Layer에 적재하는 ETL 파이프라인**입니다.

- 실행 주기: 월 1회 (예정)
- 데이터 특성: 준정적 reference data
- 출력: S3 Bronze (raw snapshot)

---

## 🏗️ Architecture

Extract → Transform → Validate → Load (S3 Bronze)

- Extract: KCIA 웹사이트 크롤링
- Transform: 최소 전처리 (Bronze 기준)
- Validate: 데이터 품질 검증
- Load: S3 업로드

---

## 📂 Directory Structure

```
kcia_pipeline/
	    |-	app.py
  		|-	config.py
	  	|-  models.py
			|-  extract.py
			|-  transform.py
			|-  validate.py
			|-  load_s3.py
			|-  http_client.py
			|-  parser.py
utils/
		logging_utils.py
```

---

## 📄 File Description

### 🔹 app.py

파이프라인의 엔트리포인트

- 전체 ETL 파이프라인 실행
- 실행 순서:
    1. extract
    2. transform
    3. validate
    4. save local
    5. upload S3

---

### 🔹 config.py

환경 변수 및 설정 관리

- `.env` 또는 환경변수에서 설정 로드
- 주요 설정:
    - KCIA_BASE_URL
    - S3_BUCKET
    - S3_PREFIX
    - ingest_date
    - batch_id

---

### 🔹 models.py

데이터 구조 정의

- `KciaRawRow`: 파싱 직후 원본 데이터
- `KciaBronzeRow`: Bronze 적재용 데이터
- `CrawlStats`: 크롤링 통계
- `ValidationResult`: 검증 결과

---

### 🔹 extract.py

데이터 수집 (Extract 단계)

- KCIA 전체 페이지 크롤링
- 페이지네이션 처리
- HTML → RawRow 변환
- 반환:
    - raw rows
    - crawl stats

---

### 🔹 transform.py

최소 전처리 (Transform 단계)

- 공백 제거
- NULL 정리
- 중복 제거 (ingredient_code 기준)
- 메타 컬럼 추가:
    - source
    - ingest_date
    - batch_id

👉 Bronze 레이어이므로 최소한의 변환만 수행

---

### 🔹 valid.py

데이터 검증

- row 수 0 여부 확인
- total count 일치 여부 확인
- (optional) strict mode 지원

👉 배치 파이프라인 안정성 확보 목적

---

### 🔹 load_s3.py

S3 업로드

- boto3 기반 업로드
- 최종 CSV → S3 Bronze 저장
s3://{bucket}/bronze/raw/kcia/ingest_date=YYYY-MM-DD/kcia.csv

---

### 🔹 http_client.py

HTTP 요청 모듈

- requests 기반
- retry / timeout 처리
- user-agent 설정
- 안정적인 크롤링 지원

---

### 🔹 parser.py

HTML 파싱 로직

- 총 건수 파싱
- 결과 테이블 탐색
- row 추출 및 구조화
- KCIA 페이지 구조에 맞춘 전용 파서

---

### 🔹 utils/logging_utils.py

로깅 설정

- 표준 logging wrapper
- timestamp + level + module 출력
- 중복 handler 방지

---
## ⚙️ Environment Variables
```
KCIA_BASE_URL=https://kcia.or.kr/cid/search/ingd_list.php
S3_BUCKET=your-bucket-name
S3_PREFIX=INCI_data/kcia

REQUEST_SLEEP=0.3
TIMEOUT=15
MAX_RETRIES=5
STRICT_COUNT_CHECK=true
```

---
## 🚀 How to Run
```bash
python -m kcia_pipeline.app
```

## 📊 Output
- Local
output/kcia_YYYY-MM-DD.csv

- S3
bronze/raw/kcia/ingest_date=YYYY-MM-DD/kcia.csv