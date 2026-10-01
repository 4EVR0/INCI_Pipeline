from __future__ import annotations

import re

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process


COSING_KEEP_COLS = [
    "substance_id",
    "inci_name",
    "cas_no",
    "function_names",
    "cosmetic_restriction",
    "other_restrictions",
    "identified_ingredient",
    "status",
    "source",
    "ingest_date",
    "batch_id",
]

CAS_PATTERN = re.compile(r"\b\d{2,7}-\d{2}-\d\b")
CAS_OVERLAP_ENABLED_MATCH_TYPES = {
    "exact_basic",
    "exact_full_normalized",
}

# 식물 유래 성분은 부위·형태가 달라도 같은 generic CAS를 공유한다
# (예: ALOE BARBADENSIS LEAF EXTRACT / LEAF WATER / LEAF JUICE).
# CAS만으로 매칭하면 서로 다른 INCI가 하나로 뭉개지므로, 이름에 부위·형태 토큰이
# 있으면 양쪽이 일치할 때만 CAS 매칭을 승인한다.
PLANT_PART_TOKENS = {
    "aerial", "bark", "bean", "berry", "bran", "branch", "bud", "bulb", "callus",
    "cone", "corm", "flower", "fruit", "germ", "hull", "husk", "kernel", "leaf",
    "meristem", "needle", "nut", "peel", "petal", "pod", "pollen", "pulp", "rhizome",
    "rind", "root", "seed", "shell", "shoot", "sprout", "stalk", "stem", "tuber",
    "twig", "vine", "wood",
}
PLANT_FORM_TOKENS = {
    "absolute", "butter", "distillate", "extract", "ferment", "filtrate", "flour",
    "gum", "juice", "lysate", "meal", "oil", "oleoresin", "powder", "resin", "sap",
    "starch", "tincture", "water", "wax",
}
_TOKEN_ALIASES = {"leaves": "leaf"}

CAS_REVIEW_REASON = "exact_cas_part_form_mismatch"


def _name_tokens(name) -> list[str]:
    if pd.isna(name):
        return []
    text = re.sub(r"\(.*?\)", " ", str(name).lower())
    tokens = re.findall(r"[a-z0-9]+", text)
    out = []
    for token in tokens:
        token = _TOKEN_ALIASES.get(token, token)
        if token not in PLANT_PART_TOKENS | PLANT_FORM_TOKENS and token.endswith("s"):
            singular = token[:-1]
            if singular in PLANT_PART_TOKENS | PLANT_FORM_TOKENS:
                token = singular
        out.append(token)
    return out


def plant_signature(name) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """이름을 (부위, 형태, 나머지 식별 토큰) 집합으로 분해한다."""
    tokens = _name_tokens(name)
    parts = frozenset(t for t in tokens if t in PLANT_PART_TOKENS)
    forms = frozenset(t for t in tokens if t in PLANT_FORM_TOKENS)
    identity = frozenset(
        t for t in tokens if t not in PLANT_PART_TOKENS and t not in PLANT_FORM_TOKENS
    )
    return parts, forms, identity


def _identity_overlaps(left: frozenset[str], right: frozenset[str]) -> bool:
    # 학명 속명 표기 차이(ARGAN / ARGANIA)를 허용하기 위해 4글자 이상 접두 일치도 인정한다.
    for a in left:
        for b in right:
            if a == b:
                return True
            if len(a) >= 4 and len(b) >= 4 and (a.startswith(b) or b.startswith(a)):
                return True
    return False


def is_cas_name_compatible(eng_name, inci_name) -> bool:
    """
    CAS가 같은 두 이름이 같은 성분으로 볼 수 있는지 판정한다.

    - 양쪽 모두 부위/형태 토큰이 없으면(일반 화합물) CAS 일치만으로 승인한다.
    - 한쪽이라도 부위/형태 토큰이 있으면 부위 집합과 형태 집합이 정확히 같아야 하고,
      나머지 식별 토큰(학명 등)도 하나 이상 겹쳐야 한다.
    """
    kcia_parts, kcia_forms, kcia_identity = plant_signature(eng_name)
    cosing_parts, cosing_forms, cosing_identity = plant_signature(inci_name)

    if not (kcia_parts or kcia_forms or cosing_parts or cosing_forms):
        return True
    if kcia_parts != cosing_parts or kcia_forms != cosing_forms:
        return False
    return _identity_overlaps(kcia_identity, cosing_identity)


def deduplicate_cosing(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    deduped = df[df[key_col] != ""].drop_duplicates(subset=[key_col]).copy()
    return deduped


def _prepare_right_df(right_df: pd.DataFrame, right_key: str, left_key: str) -> pd.DataFrame:
    return right_df[[right_key] + COSING_KEEP_COLS].rename(
        columns={
            right_key: left_key,
            "cas_no": "cosing_cas_no",
            "source": "cosing_source",
            "ingest_date": "cosing_ingest_date",
            "batch_id": "cosing_batch_id",
        }
    )


def _rename_kcia_meta_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "source" in out.columns:
        out = out.rename(columns={"source": "kcia_source"})
    if "ingest_date" in out.columns:
        out = out.rename(columns={"ingest_date": "kcia_ingest_date"})
    if "batch_id" in out.columns:
        out = out.rename(columns={"batch_id": "kcia_batch_id"})
    return out


def _extract_cas_set(value) -> set[str]:
    if pd.isna(value):
        return set()
    return set(CAS_PATTERN.findall(str(value)))


def _has_cas_overlap(kcia_cas_raw, cosing_cas_raw) -> bool:
    kcia_set = _extract_cas_set(kcia_cas_raw)
    cosing_set = _extract_cas_set(cosing_cas_raw)
    return len(kcia_set & cosing_set) > 0


def _build_match_type_series(
    df: pd.DataFrame,
    base_match_type: str,
    accepted_mask: pd.Series,
    overlap_mask: pd.Series,
) -> pd.Series:
    match_type = pd.Series("", index=df.index, dtype="object")
    match_type.loc[accepted_mask] = base_match_type

    if base_match_type in CAS_OVERLAP_ENABLED_MATCH_TYPES:
        match_type.loc[overlap_mask] = f"{base_match_type}_cas_overlap"

    return match_type


def _split_by_cas_consistency(
    df: pd.DataFrame,
    match_type: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    반환:
    - accepted: 자동 승인 가능한 매칭
    - cas_conflict_review: 이름은 맞았지만 CAS가 서로 달라 review로 보내야 하는 행
    - unmatched: 애초에 CosIng 매칭이 안 된 행
    """
    out = df.copy()

    for col in [
        "inci_name",
        "cosing_cas_no",
        "substance_id",
        "function_names",
        "cosmetic_restriction",
        "other_restrictions",
        "identified_ingredient",
        "status",
        "cosing_source",
        "cosing_ingest_date",
        "cosing_batch_id",
    ]:
        if col in out.columns:
            out[col] = out[col].fillna("")
        else:
            out[col] = ""

    has_match = out["inci_name"].ne("")

    kcia_cas_norm = out["key_cas"].fillna("").astype(str).str.strip()
    cosing_cas_norm = out["cosing_cas_no"].fillna("").astype(str).str.strip()

    both_have_cas = kcia_cas_norm.ne("") & cosing_cas_norm.ne("")
    cas_exact_match = both_have_cas & (kcia_cas_norm == cosing_cas_norm)

    if match_type in CAS_OVERLAP_ENABLED_MATCH_TYPES:
        cas_overlap = pd.Series(
            [
                _has_cas_overlap(kcia_raw, cosing_raw)
                for kcia_raw, cosing_raw in zip(out["cas_no"], out["cosing_cas_no"])
            ],
            index=out.index,
        )
    else:
        cas_overlap = pd.Series(False, index=out.index)

    cas_overlap_match = has_match & both_have_cas & ~cas_exact_match & cas_overlap
    cas_conflict = has_match & both_have_cas & ~cas_exact_match & ~cas_overlap

    accepted_mask = has_match & ~cas_conflict

    out["match_type"] = _build_match_type_series(
        df=out,
        base_match_type=match_type,
        accepted_mask=accepted_mask,
        overlap_mask=cas_overlap_match,
    )
    out["match_score"] = np.where(accepted_mask, 100.0, np.nan)

    accepted = out[accepted_mask].copy()
    cas_conflict_review = out[cas_conflict].copy()
    unmatched = out[~has_match].copy()

    if not cas_conflict_review.empty:
        cas_conflict_review["review_decision"] = "pending"
        cas_conflict_review["review_reason"] = f"{match_type}_cas_conflict"
        cas_conflict_review["candidate_score"] = 100.0
        cas_conflict_review["candidate_inci_name"] = cas_conflict_review["inci_name"]

    return accepted, cas_conflict_review, unmatched


def exact_match(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_key: str,
    right_key: str,
    match_type: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    반환:
    - exact_matched
    - cas_conflict_review
    - unmatched
    """
    left_df = left_df.loc[:, ~left_df.columns.duplicated()].copy()
    right_prepared = _prepare_right_df(right_df, right_key, left_key)

    merged = left_df.merge(
        right_prepared,
        on=left_key,
        how="left",
    )
    merged = _rename_kcia_meta_cols(merged)

    exact_matched, cas_conflict_review, unmatched = _split_by_cas_consistency(
        merged,
        match_type=match_type,
    )
    return exact_matched, cas_conflict_review, unmatched


def _attach_cosing(left_df: pd.DataFrame, cosing_df: pd.DataFrame, cosing_idx: pd.Series) -> pd.DataFrame:
    right = _prepare_right_df(cosing_df.loc[cosing_idx.to_numpy()], "key_cas", "key_cas")
    right = right.drop(columns=["key_cas"]).set_index(cosing_idx.index)
    out = pd.concat([left_df.loc[cosing_idx.index], right], axis=1)
    return _rename_kcia_meta_cols(out)


def cas_match(
    left_df: pd.DataFrame,
    cosing_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    key_cas 기준 매칭. 같은 CAS를 가진 CosIng 후보 전체를 보고 다음 순서로 고른다.

    1. 정규화 이름(key_basic)까지 같은 후보 → 승인
    2. KCIA 이름이 CosIng 어딘가에 이름 그대로 존재 → 이름 매칭 단계로 넘김
    3. 부위·형태가 호환되는 첫 후보 → 승인 (일반 화합물은 기존처럼 첫 후보)
    4. 호환 후보 없음 → 이름 매칭 단계로 넘기고, 끝까지 못 찾으면 review로 보낸다

    반환:
    - exact_matched
    - cas_rejected: 4번에 해당하는 행 + 거절된 첫 CAS 후보 (review 후보)
    - unmatched: 2·4번 및 CAS 후보가 없는 행
    """
    left_df = left_df.loc[:, ~left_df.columns.duplicated()].copy()

    cosing_with_cas = cosing_df[cosing_df["key_cas"] != ""].drop_duplicates(
        subset=["key_cas", "inci_name"]
    )
    candidates_by_cas: dict[str, list] = {}
    for idx, cas, key_basic, inci_name in zip(
        cosing_with_cas.index,
        cosing_with_cas["key_cas"],
        cosing_with_cas["key_basic"],
        cosing_with_cas["inci_name"],
    ):
        candidates_by_cas.setdefault(cas, []).append((idx, key_basic, inci_name))

    cosing_name_keys = set(cosing_df["key_basic"]) | set(cosing_df["key_full"])
    cosing_name_keys.discard("")

    chosen: dict = {}
    rejected: dict = {}
    for row_idx, cas, key_basic, key_full, eng_name in zip(
        left_df.index,
        left_df["key_cas"].fillna(""),
        left_df["key_basic"].fillna(""),
        left_df["key_full"].fillna(""),
        left_df["std_name_en"],
    ):
        candidates = candidates_by_cas.get(cas) if cas else None
        if not candidates:
            continue

        same_name = next((c for c in candidates if key_basic and c[1] == key_basic), None)
        if same_name is not None:
            chosen[row_idx] = same_name[0]
            continue

        if key_basic in cosing_name_keys or key_full in cosing_name_keys:
            continue

        compatible = next(
            (c for c in candidates if is_cas_name_compatible(eng_name, c[2])), None
        )
        if compatible is not None:
            chosen[row_idx] = compatible[0]
        else:
            rejected[row_idx] = candidates[0][0]

    chosen_idx = pd.Series(chosen, dtype="object")
    rejected_idx = pd.Series(rejected, dtype="object")

    exact_matched = _attach_cosing(left_df, cosing_df, chosen_idx)
    exact_matched["match_type"] = "exact_cas"
    exact_matched["match_score"] = 100.0

    cas_rejected = _attach_cosing(left_df, cosing_df, rejected_idx)
    cas_rejected["review_decision"] = "pending"
    cas_rejected["review_reason"] = CAS_REVIEW_REASON
    cas_rejected["candidate_score"] = 100.0
    cas_rejected["candidate_inci_name"] = cas_rejected["inci_name"]

    unmatched = _rename_kcia_meta_cols(left_df.drop(index=chosen_idx.index))
    return exact_matched, cas_rejected, unmatched


def fuzzy_match_one(name: str, candidates: list[str], score_cutoff: int):
    if not name:
        return None, None

    result = process.extractOne(
        query=name,
        choices=candidates,
        scorer=fuzz.ratio,
        score_cutoff=score_cutoff,
    )

    if result:
        matched_key, score, _ = result
        return matched_key, score

    return None, None


def fuzzy_match_dataframe(
    unmatched_df: pd.DataFrame,
    cosing_df: pd.DataFrame,
    source_key_col: str,
    auto_threshold: int,
    review_threshold: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    work = unmatched_df.loc[:, ~unmatched_df.columns.duplicated()].copy()

    drop_cols = [
        c
        for c in COSING_KEEP_COLS + ["cosing_cas_no", "match_type", "match_score"]
        if c in work.columns
    ]
    work = work.drop(columns=drop_cols, errors="ignore")

    cosing_map = (
        cosing_df[[source_key_col] + COSING_KEEP_COLS]
        .dropna(subset=[source_key_col])
        .query(f"{source_key_col} != ''")
        .drop_duplicates(subset=[source_key_col])
        .copy()
    )
    candidates = cosing_map[source_key_col].tolist()

    work[["fuzzy_key_auto", "fuzzy_score_auto"]] = work[source_key_col].apply(
        lambda x: pd.Series(fuzzy_match_one(x, candidates, auto_threshold))
    )
    work[["fuzzy_key_review", "fuzzy_score_review"]] = work[source_key_col].apply(
        lambda x: pd.Series(fuzzy_match_one(x, candidates, review_threshold))
    )

    auto = work[work["fuzzy_key_auto"].notna()].copy()
    auto = auto.merge(
        cosing_map.rename(
            columns={
                source_key_col: "fuzzy_key_auto",
                "cas_no": "cosing_cas_no",
                "source": "cosing_source",
                "ingest_date": "cosing_ingest_date",
                "batch_id": "cosing_batch_id",
            }
        ),
        on="fuzzy_key_auto",
        how="left",
    )

    # fuzzy auto는 기존처럼 보수적으로 유지
    auto["inci_name"] = auto["inci_name"].fillna("")
    auto["cosing_cas_no"] = auto["cosing_cas_no"].fillna("")
    auto["key_cas"] = auto["key_cas"].fillna("")

    both_have_cas = auto["key_cas"].astype(str).str.strip().ne("") & auto["cosing_cas_no"].astype(str).str.strip().ne("")
    cas_overlap = pd.Series(
        [
            _has_cas_overlap(kcia_raw, cosing_raw)
            for kcia_raw, cosing_raw in zip(auto["key_cas"], auto["cosing_cas_no"])
        ],
        index=auto.index,
    )
    cas_mismatch = both_have_cas & (
        auto["key_cas"].astype(str).str.strip() != auto["cosing_cas_no"].astype(str).str.strip()
    ) & ~cas_overlap

    auto_accepted = auto[~cas_mismatch].copy()
    auto_accepted["match_type"] = "fuzzy_auto"
    auto_accepted["match_score"] = auto_accepted["fuzzy_score_auto"]

    auto_cas_conflict_review = auto[cas_mismatch].copy()
    if not auto_cas_conflict_review.empty:
        auto_cas_conflict_review["review_decision"] = "pending"
        auto_cas_conflict_review["review_reason"] = "fuzzy_auto_cas_conflict"
        auto_cas_conflict_review["candidate_score"] = auto_cas_conflict_review["fuzzy_score_auto"]
        auto_cas_conflict_review["candidate_inci_name"] = auto_cas_conflict_review["inci_name"]

    review = work[
        work["fuzzy_key_auto"].isna() & work["fuzzy_key_review"].notna()
    ].copy()
    review = review.merge(
        cosing_map.rename(
            columns={
                source_key_col: "fuzzy_key_review",
                "cas_no": "cosing_cas_no",
                "source": "cosing_source",
                "ingest_date": "cosing_ingest_date",
                "batch_id": "cosing_batch_id",
            }
        ),
        on="fuzzy_key_review",
        how="left",
    )
    review["review_decision"] = "pending"
    review["review_reason"] = "fuzzy_review_threshold"
    review["candidate_score"] = review["fuzzy_score_review"]
    review["candidate_inci_name"] = review["inci_name"]

    review_all = pd.concat([review, auto_cas_conflict_review], ignore_index=True)

    still_unmatched = work[
        work["fuzzy_key_auto"].isna() & work["fuzzy_key_review"].isna()
    ].copy()

    return auto_accepted, review_all, still_unmatched