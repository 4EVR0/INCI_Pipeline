from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

GOLD_COLUMN_ORDER = [
    "inci_name",
    "kor_name",
    "eng_name",
    "ingredient_code",
    "kcia_cas_no",
    "cosing_functions",
    "status",
    "cosmetic_restriction",
    "other_restrictions",
    "match_type",
    "match_score",
    "is_fuzzy",
]


def _normalize_cosing_functions(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = str(raw).strip()
    if not s:
        return ""
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


def _clean_str(series: pd.Series) -> pd.Series:
    return series.astype(str).replace({"nan": "", "None": ""}).str.strip().replace({"": pd.NA})


def transform_to_gold(graphrag_df: pd.DataFrame) -> pd.DataFrame:
    """
    graphrag_map → Gold ingredients

    graphrag_map은 전체 KCIA 성분을 포함합니다:
      - exact_*  : 정확 매칭
      - fuzzy_*  : 퍼지 매칭 (is_fuzzy=True)
      - kcia_only: INCI 매핑 없는 성분 (inci_name=None)
    """
    g = graphrag_df.copy()
    g["match_score"] = pd.to_numeric(g.get("match_score"), errors="coerce")
    g["is_fuzzy"] = g.get("is_fuzzy", False).apply(
        lambda v: bool(v) if isinstance(v, bool)
        else (str(v).strip().upper() == "TRUE" if pd.notna(v) else False)
    )

    # 동일 ingredient_code 중 match_score 높은 것 유지
    g = g.sort_values("match_score", ascending=False, na_position="last", kind="mergesort")
    g = g.drop_duplicates(subset=["ingredient_code"], keep="first")

    inci = _clean_str(g.get("canonical_inci_name", pd.Series(dtype=str)))
    # kcia_only는 canonical_inci_name이 비어 있음 → NA 유지

    out = pd.DataFrame({
        "inci_name":            inci,
        "kor_name":             _clean_str(g.get("std_name_ko", pd.Series(dtype=str))),
        "eng_name":             _clean_str(g.get("std_name_en", pd.Series(dtype=str))),
        "ingredient_code":      _clean_str(g.get("ingredient_code", pd.Series(dtype=str))),
        "kcia_cas_no":          _clean_str(g.get("kcia_cas_no", pd.Series(dtype=str))),
        "cosing_functions":     g.get("function_names", pd.Series(dtype=str)).map(_normalize_cosing_functions).replace({"": pd.NA}),
        "status":               _clean_str(g.get("status", pd.Series(dtype=str))),
        "cosmetic_restriction": _clean_str(g.get("cosmetic_restriction", pd.Series(dtype=str))),
        "other_restrictions":   _clean_str(g.get("other_restrictions", pd.Series(dtype=str))),
        "match_type":           _clean_str(g.get("match_type", pd.Series(dtype=str))),
        "match_score":          g["match_score"],
        "is_fuzzy":             g["is_fuzzy"],
    })

    return out[GOLD_COLUMN_ORDER].reset_index(drop=True)


def load_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna("")


# 하위 호환용 alias
def load_matched_final_csv(path: str | Path) -> pd.DataFrame:
    return load_csv(path)


def transform_matched_final_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    return transform_to_gold(df)
