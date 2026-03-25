from __future__ import annotations

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


def deduplicate_cosing(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    deduped = df[df[key_col] != ""].drop_duplicates(subset=[key_col]).copy()
    return deduped


def exact_match(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_key: str,
    right_key: str,
    match_type: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    left_df = left_df.loc[:, ~left_df.columns.duplicated()].copy()
    right_prepared = right_df[[right_key] + COSING_KEEP_COLS].rename(
        columns={
            right_key: left_key,
            "cas_no": "cosing_cas_no",
            "source": "cosing_source",
            "ingest_date": "cosing_ingest_date",
            "batch_id": "cosing_batch_id",
        }
    )
    matched = left_df.merge(
        right_prepared,
        on=left_key,
        how="left",
    )

    for col in [
        "substance_id",
        "inci_name",
        "cosing_cas_no",
        "function_names",
        "cosmetic_restriction",
        "other_restrictions",
        "identified_ingredient",
        "status",
        "cosing_source",
        "cosing_ingest_date",
        "cosing_batch_id",
    ]:
        if col in matched.columns:
            matched[col] = matched[col].fillna("")

    if "source" in matched.columns:
        matched = matched.rename(columns={"source": "kcia_source"})
    if "ingest_date" in matched.columns:
        matched = matched.rename(columns={"ingest_date": "kcia_ingest_date"})
    if "batch_id" in matched.columns:
        matched = matched.rename(columns={"batch_id": "kcia_batch_id"})

    matched["match_type"] = np.where(matched["inci_name"].ne(""), match_type, "")
    matched["match_score"] = np.where(matched["inci_name"].ne(""), 100.0, np.nan)

    exact_matched = matched[matched["inci_name"].ne("")].copy()
    unmatched = matched[matched["inci_name"].eq("")].copy()
    return exact_matched, unmatched


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
    drop_cols = [c for c in COSING_KEEP_COLS + ["cosing_cas_no", "match_type", "match_score"] if c in work.columns]
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
            columns={source_key_col: "fuzzy_key_auto", "cas_no": "cosing_cas_no", "source": "cosing_source", "ingest_date": "cosing_ingest_date", "batch_id": "cosing_batch_id"}
        ),
        on="fuzzy_key_auto",
        how="left",
    )
    auto["match_type"] = "fuzzy_auto"
    auto["match_score"] = auto["fuzzy_score_auto"]

    review = work[
        work["fuzzy_key_auto"].isna() & work["fuzzy_key_review"].notna()
    ].copy()
    review = review.merge(
        cosing_map.rename(
            columns={source_key_col: "fuzzy_key_review", "cas_no": "cosing_cas_no", "source": "cosing_source", "ingest_date": "cosing_ingest_date", "batch_id": "cosing_batch_id"}
        ),
        on="fuzzy_key_review",
        how="left",
    )
    review["review_decision"] = "pending"
    review["candidate_score"] = review["fuzzy_score_review"]

    still_unmatched = work[
        work["fuzzy_key_auto"].isna() & work["fuzzy_key_review"].isna()
    ].copy()

    return auto, review, still_unmatched
