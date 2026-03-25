# KCIA ↔ CosIng Silver Mapping Pipeline

브론즈에 적재된 KCIA / CosIng CSV를 읽어서 실버 매핑 결과를 생성합니다.

## 주요 출력

- `kcia_cosing_matched_final.csv`: 자동 매핑 성공 결과 전체
- `kcia_cosing_fuzzy_review_latest.csv`: 수동 검토가 필요한 fuzzy 후보
- `kcia_cosing_unmatched_final.csv`: 끝까지 매칭되지 않은 KCIA 성분
- `kcia_cosing_graphrag_map.csv`: GraphRAG 적재용 최소 컬럼셋
- `mapping_summary.csv`: 매핑 통계

## 매칭 우선순위

1. CAS exact
2. basic exact
3. normalized exact
4. parentheses removed exact
5. word sorted exact
6. strict word sorted exact
7. fuzzy auto / fuzzy review

## 실행

### 로컬 CSV 기준

```bash
pip install -r requirements.txt
cp .env.example .env
python -m kcia_cosing_silver.run_mapping
```

### S3 브론즈 기준

`.env`에서 아래처럼 바꾸면 됩니다.

```bash
MAPPING_INPUT_MODE=s3
S3_BUCKET=oliveyoung-crawl-data
KCIA_S3_PREFIX=INCI_data/kcia
COSING_S3_PREFIX=INCI_data/cosing
AWS_DEFAULT_REGION=ap-northeast-2
```

특정 ingest_date만 읽고 싶으면:

```bash
KCIA_INGEST_DATE=2026-03-24
COSING_INGEST_DATE=2026-03-24
```

## GraphRAG 컬럼셋

`kcia_cosing_graphrag_map.csv`에는 아래 컬럼만 남깁니다.

- ingredient_code
- std_name_ko
- std_name_en
- old_name_ko
- kcia_cas_no
- canonical_inci_name
- cosing_substance_id
- cosing_cas_no
- function_names
- cosmetic_restriction
- other_restrictions
- identified_ingredient
- status
- match_type
- match_score

이 컬럼셋은 이후 그래프 엔티티/엣지 구성에 바로 쓰기 좋게 최소화한 버전입니다.
