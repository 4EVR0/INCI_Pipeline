from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

# matched_final → gold ingredient (gold_schema.txt)
KEEP_SILVER_COLS = [
    "canonical_inci_name",
    "std_name_ko",
    "std_name_en",
    "function_names",
    "status",
    "cosmetic_restriction",
    "other_restrictions",
    "match_score",
]

GOLD_COLUMN_ORDER = [
    "inci_name",
    "kor_name",
    "eng_name",
    "cosing_functions",
    "status",
    "cosmetic_restriction",
    "other_restrictions",
]


def _normalize_cosing_functions(raw: object) -> str:
    """세미콜론 기준 분리 + strip + 빈값 제거 + 중복 제거; CSV에는 ; 로 join (스키마 권장)."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    # 원본에 | 구분이 많아 ; 와 | 를 동일한 구분자로 취급한 뒤 ; 로 재조합
    parts = re.split(r"[;|]", s)
    seen: set[str] = set()
    ordered: list[str] = []
    for p in parts:
        t = p.strip()
        if not t:
            continue
        key = t.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(t)
    return ";".join(ordered)


def transform_matched_final_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in KEEP_SILVER_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"matched_final에 필요한 컬럼이 없습니다: {missing}")

    work = df[KEEP_SILVER_COLS].copy()

    inci = work["canonical_inci_name"].astype(str)
    mask = work["canonical_inci_name"].notna() & (inci.str.strip() != "") & (inci != "nan")
    work = work.loc[mask].copy()

    work["match_score"] = pd.to_numeric(work["match_score"], errors="coerce")

    work["_has_fn"] = work["function_names"].notna() & (
        work["function_names"].astype(str).str.strip().ne("") & work["function_names"].astype(str).ne("nan")
    )
    work["_has_ko"] = work["std_name_ko"].notna() & (
        work["std_name_ko"].astype(str).str.strip().ne("") & work["std_name_ko"].astype(str).ne("nan")
    )
    work["_has_en"] = work["std_name_en"].notna() & (
        work["std_name_en"].astype(str).str.strip().ne("") & work["std_name_en"].astype(str).ne("nan")
    )

    work = work.sort_values(
        by=["match_score", "_has_fn", "_has_ko", "_has_en"],
        ascending=[False, False, False, False],
        kind="mergesort",
    )
    work = work.drop_duplicates(subset=["canonical_inci_name"], keep="first")

    out = pd.DataFrame(
        {
            "inci_name": work["canonical_inci_name"].astype(str).str.strip(),
            "kor_name": work["std_name_ko"].astype(str).replace({"nan": ""}).str.strip(),
            "eng_name": work["std_name_en"].astype(str).replace({"nan": ""}).str.strip(),
            "cosing_functions": work["function_names"].map(_normalize_cosing_functions),
            "status": work["status"].astype(str).replace({"nan": ""}).str.strip(),
            "cosmetic_restriction": work["cosmetic_restriction"]
            .astype(str)
            .replace({"nan": ""})
            .str.strip(),
            "other_restrictions": work["other_restrictions"].astype(str).replace({"nan": ""}).str.strip(),
        }
    )

    for col in (
        "kor_name",
        "eng_name",
        "cosing_functions",
        "status",
        "cosmetic_restriction",
        "other_restrictions",
    ):
        out[col] = out[col].replace("", pd.NA)

    return out[GOLD_COLUMN_ORDER].reset_index(drop=True)


def load_matched_final_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna("")
