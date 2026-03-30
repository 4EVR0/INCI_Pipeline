import pandas as pd

def clean_id(series):
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
    )

# 파일 로드
kcia = pd.read_csv("kcia_ingredient_dict_rebuilt.csv")
matched = pd.read_csv("kcia_cosing_matched_final.csv")
unmatched = pd.read_csv("kcia_cosing_unmatched_final.csv")
review = pd.read_csv("kcia_cosing_fuzzy_review_latest.csv")

# id 정규화
kcia["ingredient_code"] = clean_id(kcia["ingredient_code"])
matched["ingredient_code"] = clean_id(matched["ingredient_code"])
unmatched["ingredient_code"] = clean_id(unmatched["ingredient_code"])
review["ingredient_code"] = clean_id(review["ingredient_code"])

# 빈 값 제거
kcia_ids = set(kcia.loc[kcia["ingredient_code"] != "", "ingredient_code"])
matched_ids = set(matched.loc[matched["ingredient_code"] != "", "ingredient_code"])
unmatched_ids = set(unmatched.loc[unmatched["ingredient_code"] != "", "ingredient_code"])
review_ids = set(review.loc[review["ingredient_code"] != "", "ingredient_code"])

# 최종 집합
final_ids = matched_ids | unmatched_ids | review_ids

# 결과 출력
print("========== VALIDATION ==========")
print("kcia_total:", len(kcia_ids))
print("matched:", len(matched_ids))
print("unmatched:", len(unmatched_ids))
print("review:", len(review_ids))
print("accounted_total:", len(final_ids))
print("missing:", len(kcia_ids - final_ids))
print("mapping_rate:", round(len(matched_ids) / len(kcia_ids), 4))

print("\n========== CHECK ==========")
if len(kcia_ids) == len(final_ids):
    print("✅ ALL KCIA INGREDIENTS ACCOUNTED FOR")
else:
    print("❌ MISSING EXISTS:", len(kcia_ids - final_ids))