# CosIng Ingredient Crawler & KCIA–CosIng Mapping Pipeline

CosIng (EU Cosmetic Ingredient Database) 데이터를 수집하고 
한국 KCIA 성분 사전과 매칭하여 **통합 INCI 성분 데이터셋을 구축하는 데이터 파이프라인**
 
목표: 화장품 성분 기반 추천 시스템(KG-RAG 기반 초개인화 추천 시스템)을 위한 **성분 표준화 데이터 레이어 구축**

---

# 1. Overview

화장품 성분 데이터는 여러 기관에서 제공되며  
서로 다른 표기법과 데이터 구조를 가지고 있음.

대표적으로

- **KCIA 성분 사전 (한국)**
- **CosIng (EU Cosmetic Ingredient Database)**

두 데이터셋을 연결하면

- INCI 표준명
- CAS 번호
- 성분 기능
- 규제 정보

등을 통합적으로 활용할 수 있음.

### 파이프라인 구조

1️⃣ CosIng Ingredient 데이터 크롤링  
2️⃣ KCIA 성분 사전과 CosIng 매칭  
3️⃣ 성분명 정규화 및 fuzzy matching  
4️⃣ 고품질 성분 매핑 데이터셋 생성  

---

# 2. Data Sources

## KCIA Ingredient Dictionary

한국화장품협회 성분 사전 
~21,778 ingredients

포함 정보

- 성분 코드
- 국문명
- 영문 INCI명
- 구명칭

---

## CosIng (EU Cosmetic Ingredient Database)

https://ec.europa.eu/growth/tools-databases/cosing/

수집 결과
32,679 ingredients

---

# 3. Pipeline Architecture
KCIA Ingredient Dict
│
│
▼
CosIng Ingredient Crawler
│
▼
CosIng Ingredient Dataset
│
▼
Name Normalization
│
▼
Exact Matching
│
▼
Fuzzy Matching
│
▼
Final Mapping Table

---

# 4. CosIng Data Collection

CosIng 웹사이트는 일반적인 pagination API가 아닌  
**search API 기반 구조**를 사용함.

또한 검색 결과가 **10,000 row 제한**이 있기 때문에  
Query splitting 방식으로 전체 데이터를 수집함.

예시:
a*
b*
c*
...

→ 필요 시
c1*
c2*
...

식으로 재분할

이를 통해 **CosIng 전체 Ingredient 데이터를 안정적으로 수집**

---

# 5. Installation

Python 3.10+

```bash
git clone <repo_url>
cd CosIng
pip install -r requirements.txt
```

---
# 6. Usage
## A. CosIng Ingredient 데이터 수집
- CosIng 데이터베이스에서 Ingredient 데이터를 크롤링합니다.
```bash
python main.py
```

- 옵션 예시 (중단된 작업 이어서 실행)
```bash
python main.py --resume-count-cache
```

- 생성 파일
```
data/cosing_ingredient_only_latest_*.csv
data/cosing_ingredient_only_latest_*.parquet
```
---
## B. KCIA ↔ CosIng 성분 매핑
- KCIA 성분 사전과 CosIng 데이터를 매칭하여 최종 매핑 테이블을 생성합니다.

```bash
python -m mapping.run_mapping
```

- 생성 파일
data/output/mapping_results/
 ├─ matched_final.csv
 ├─ fuzzy_review.csv
 ├─ final_unmatched_kcia.csv
 └─ mapping_summary.csv