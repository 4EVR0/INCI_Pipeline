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

    # exact_cas는 key_cas 기준으로 이미 맞췄기 때문에 그대로 승인
    if match_type == "exact_cas":
        merged["match_type"] = np.where(merged["inci_name"].fillna("").ne(""), match_type, "")
        merged["match_score"] = np.where(merged["inci_name"].fillna("").ne(""), 100.0, np.nan)

        exact_matched = merged[merged["inci_name"].fillna("").ne("")].copy()
        unmatched = merged[merged["inci_name"].fillna("").eq("")].copy()
        cas_conflict_review = merged.iloc[0:0].copy()
        return exact_matched, cas_conflict_review, unmatched

    exact_matched, cas_conflict_review, unmatched = _split_by_cas_consistency(
        merged,
        match_type=match_type,
    )
    return exact_matched, cas_conflict_review, unmatched


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
    cas_mismatch = both_have_cas & (
        auto["key_cas"].astype(str).str.strip() != auto["cosing_cas_no"].astype(str).str.strip()
    )

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