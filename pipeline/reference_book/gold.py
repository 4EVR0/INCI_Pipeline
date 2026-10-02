"""Gold: Silver matched → 그래프 적재용 근거(성분 × 효능).

- 효능이 남아 있고 범위가 skin·general인 matched 항목만 쓴다(용도만 있는 항목·주의 문구로 막힌 효능은 Silver에서 이미 제외).
- 같은 성분·효능을 여러 항목이 말하면 skin 범위를 우선해 하나로 합치고, 출처 쪽·책 항목명을 모두 남긴다.
- 원문 정책: Gold에는 책 문장·구절을 넣지 않고 구조화된 값(효능 코드·범위·출처)만 둔다. 그래프·응답에서
  설명이 필요하면 효능 코드별로 정해진 문구를 쓴다(책 문장을 옮기지 않음).
"""

from __future__ import annotations

import pandas as pd

BOOK_CITATION = "김기연 외, 『화장품성분학 사전』, 현문사, 2011 (ISBN 9788966300891)"
EVIDENCE_TYPE = "reference_book"
GOLD_COLUMNS = ["inci_name", "effect_code", "evidence_type", "claim_scope", "medical_wording", "print_page",
                "book_kor_name", "citation"]


def build_gold(matched: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for item in matched.fillna("").to_dict("records"):
        effects = [e for e in str(item["effect_codes"]).split("|") if e]
        if item["claim_scope"] not in ("skin", "general") or not effects:
            continue
        for inci in [i for i in str(item["inci_names"]).split(" | ") if i]:
            for effect in effects:
                rows.append({
                    "inci_name": inci, "effect_code": effect, "evidence_type": EVIDENCE_TYPE,
                    "claim_scope": item["claim_scope"],
                    # 의약 표현 출처만 있는 근거는 응답 문구에서 '치료' 등을 쓰지 않도록 표시
                    "medical_wording": "medical_wording" in str(item.get("flags", "")).split("|"),
                    "print_page": str(item["print_page"]),
                    "book_kor_name": item["kor_name"], "citation": BOOK_CITATION,
                })
    evidence = pd.DataFrame(rows, columns=GOLD_COLUMNS)
    if evidence.empty:
        return evidence
    evidence["_rank"] = (evidence["claim_scope"] != "skin").astype(int)
    evidence["_page"] = evidence["print_page"].astype(int)
    return (evidence.sort_values(["inci_name", "effect_code", "_rank", "_page"])
            .groupby(["inci_name", "effect_code"], as_index=False)
            .agg({"evidence_type": "first", "claim_scope": "first", "medical_wording": "all",
                  "print_page": lambda s: "|".join(dict.fromkeys(s)),
                  "book_kor_name": lambda s: "|".join(dict.fromkeys(s)),
                  "citation": "first"})[GOLD_COLUMNS])
