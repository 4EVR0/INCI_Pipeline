import re
from pathlib import Path
import pandas as pd


# =========================
# 1. 경로 설정
# =========================
BASE_DIR = Path("/Users/hyeokjun/INCI_data/silver_mapping/data/silver")

MATCHED_PATH = BASE_DIR / "kcia_cosing_matched_final.csv"
REVIEW_PATH = BASE_DIR / "kcia_cosing_fuzzy_review_latest.csv"
UNMATCHED_PATH = BASE_DIR / "kcia_cosing_unmatched_final.csv"
SUMMARY_PATH = BASE_DIR / "mapping_summary.csv"

OUTPUT_MATCHED_PATH = BASE_DIR / "kcia_cosing_matched_final_v2.csv"
OUTPUT_REVIEW_PATH = BASE_DIR / "kcia_cosing_fuzzy_review_remaining_v2.csv"
OUTPUT_SUMMARY_PATH = BASE_DIR / "mapping_summary_v2.csv"
OUTPUT_PROMOTED_PATH = BASE_DIR / "kcia_cosing_promoted_from_review_v2.csv"


# =========================
# 2. 유틸 함수
# =========================
CAS_PATTERN = re.compile(r"\b\d{2,7}-\d{2}-\d\b")


def extract_cas_set(value) -> set[str]:
    """
    문자열 안에서 CAS 번호만 추출해서 set으로 반환
    예:
    '25584-83-2 999-61-1' -> {'25584-83-2', '999-61-1'}
    '999-61-1; 25584-83-2' -> {'999-61-1', '25584-83-2'}
    """
    if pd.isna(value):
        return set()
    return set(CAS_PATTERN.findall(str(value)))


def has_cas_overlap(kcia_cas, cosing_cas) -> bool:
    kcia_set = extract_cas_set(kcia_cas)
    cosing_set = extract_cas_set(cosing_cas)
    return len(kcia_set & cosing_set) > 0


def determine_promoted_match_type(review_reason: str) -> str:
    """
    review_reason에 따라 승격 후 match_type 이름 부여
    """
    if review_reason == "exact_basic_cas_conflict":
        return "exact_basic_cas_overlap"
    elif review_reason == "exact_full_normalized_cas_conflict":
        return "exact_full_normalized_cas_overlap"
    else:
        return "promoted_from_review"


def build_promoted_matched_df(promoted_review_df: pd.DataFrame, matched_columns: list[str]) -> pd.DataFrame:
    """
    review df를 matched_final 형식으로 변환
    """
    promoted = promoted_review_df.copy()

    # candidate_score 숫자화
    promoted["candidate_score"] = pd.to_numeric(promoted["candidate_score"], errors="coerce")

    # review -> matched 컬럼명 변환
    rename_map = {
        "candidate_inci_name": "canonical_inci_name",
        "candidate_score": "match_score",
    }
    promoted = promoted.rename(columns=rename_map)

    # match_type 재설정
    promoted["match_type"] = promoted["review_reason"].apply(determine_promoted_match_type)

    # matched_final에 있을 수 있는 부가 컬럼 기본값
    default_values = {
        "kcia_source": "kcia",
        "kcia_ingest_date": pd.NA,
        "kcia_batch_id": pd.NA,
        "cosing_source": "cosing",
        "cosing_ingest_date": pd.NA,
        "cosing_batch_id": pd.NA,
        "as_of_date": pd.NA,
    }

    for col, default_val in default_values.items():
        if col not in promoted.columns:
            promoted[col] = default_val

    # matched_final에 필요한 컬럼이 없으면 생성
    for col in matched_columns:
        if col not in promoted.columns:
            promoted[col] = pd.NA

    # 혹시라도 중복 컬럼 생겼으면 제거
    promoted = promoted.loc[:, ~promoted.columns.duplicated()]

    # matched_final 컬럼 순서로 맞춤
    promoted = promoted[matched_columns]

    return promoted

# =========================
# 3. 데이터 로드
# =========================
matched_df = pd.read_csv(MATCHED_PATH)
review_df = pd.read_csv(REVIEW_PATH)
unmatched_df = pd.read_csv(UNMATCHED_PATH)
summary_df = pd.read_csv(SUMMARY_PATH)

print("=== INPUT COUNTS ===")
print(f"matched_final: {len(matched_df):,}")
print(f"review:        {len(review_df):,}")
print(f"unmatched:     {len(unmatched_df):,}")
print()


# =========================
# 4. 승격 대상 판정
# =========================
PROMOTABLE_REASONS = {
    "exact_basic_cas_conflict",
    "exact_full_normalized_cas_conflict",
}

review_df["candidate_score"] = pd.to_numeric(review_df["candidate_score"], errors="coerce")
review_df["cas_overlap"] = review_df.apply(
    lambda row: has_cas_overlap(row["kcia_cas_no"], row["cosing_cas_no"]),
    axis=1,
)

promote_mask = (
    review_df["review_reason"].isin(PROMOTABLE_REASONS)
    & review_df["cas_overlap"]
)

promoted_review_df = review_df.loc[promote_mask].copy()
remaining_review_df = review_df.loc[~promote_mask].copy()

print("=== REVIEW REASON DISTRIBUTION ===")
print(review_df["review_reason"].value_counts(dropna=False))
print()

print("=== PROMOTION RESULT ===")
print(f"promoted from review: {len(promoted_review_df):,}")
print(f"remaining review:     {len(remaining_review_df):,}")
print()




# =========================
# 5. review -> matched 형식으로 변환
# =========================
matched_columns = matched_df.columns.tolist()
promoted_matched_df = build_promoted_matched_df(promoted_review_df, matched_columns)

# 혹시 ingredient_code 중복 방지
existing_codes = set(matched_df["ingredient_code"].tolist())
promoted_matched_df = promoted_matched_df[
    ~promoted_matched_df["ingredient_code"].isin(existing_codes)
].copy()

new_matched_df = pd.concat([matched_df, promoted_matched_df], ignore_index=True)

# review 파일에는 분석용 컬럼 cas_overlap 남겨도 되지만,
# 원본 형식 최대한 유지하려면 제거 가능
# remaining_review_df = remaining_review_df.drop(columns=["cas_overlap"], errors="ignore")


# =========================
# 6. summary 재계산
# =========================
kcia_total = int(summary_df.loc[0, "kcia_total"])
new_matched_total = len(new_matched_df)
new_review_total = len(remaining_review_df)
unmatched_total = len(unmatched_df)

accounted_total = new_matched_total + new_review_total + unmatched_total
missing_total = kcia_total - accounted_total

match_rate_vs_kcia = round(new_matched_total / kcia_total * 100, 2)
coverage_excluding_review = round((new_matched_total + unmatched_total) / kcia_total * 100, 2)
coverage_including_review = round((new_matched_total + new_review_total + unmatched_total) / kcia_total * 100, 2)

new_summary_df = summary_df.copy()
new_summary_df.loc[0, "matched_final_total"] = new_matched_total
new_summary_df.loc[0, "fuzzy_review_total"] = new_review_total
new_summary_df.loc[0, "unmatched_kcia_total"] = unmatched_total
new_summary_df.loc[0, "accounted_total"] = accounted_total
new_summary_df.loc[0, "missing_total"] = missing_total
new_summary_df.loc[0, "match_rate_vs_kcia"] = match_rate_vs_kcia
new_summary_df.loc[0, "coverage_excluding_review"] = coverage_excluding_review
new_summary_df.loc[0, "coverage_including_review"] = coverage_including_review


# =========================
# 7. 파일 저장
# =========================
new_matched_df.to_csv(OUTPUT_MATCHED_PATH, index=False)
remaining_review_df.to_csv(OUTPUT_REVIEW_PATH, index=False)
new_summary_df.to_csv(OUTPUT_SUMMARY_PATH, index=False)

# 승격 샘플 확인용 파일
promoted_review_df.to_csv(OUTPUT_PROMOTED_PATH, index=False)


# =========================
# 8. 결과 출력
# =========================
print("=== OUTPUT COUNTS ===")
print(f"new matched_final: {len(new_matched_df):,}")
print(f"new review:        {len(remaining_review_df):,}")
print(f"unmatched:         {len(unmatched_df):,}")
print(f"accounted_total:   {accounted_total:,}")
print(f"missing_total:     {missing_total:,}")
print(f"new match_rate:    {match_rate_vs_kcia}%")
print()

print("=== SAVED FILES ===")
print(OUTPUT_MATCHED_PATH)
print(OUTPUT_REVIEW_PATH)
print(OUTPUT_SUMMARY_PATH)
print(OUTPUT_PROMOTED_PATH)