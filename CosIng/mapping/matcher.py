import pandas as pd
from rapidfuzz import fuzz, process


def deduplicate_cosing(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    """
    같은 key가 여러 개면 첫 row만 대표값으로 사용
    """
    deduped = (
        df.dropna(subset=[key_col])
          .drop_duplicates(subset=[key_col])
          .copy()
    )
    return deduped


def exact_match(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_key: str,
    right_key: str,
    match_type: str,
) -> pd.DataFrame:
    """
    left_df 기준으로 right_df를 조인
    """
    matched = left_df.merge(
        right_df[[right_key, "inci_name"]].rename(columns={right_key: left_key}),
        on=left_key,
        how="left",
    )
    matched["match_type"] = matched["inci_name"].notna().map(
        lambda x: match_type if x else None
    )
    return matched


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


def deduplicate_cosing(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    deduped = (
        df.dropna(subset=[key_col])
          .drop_duplicates(subset=[key_col])
          .copy()
    )
    return deduped


def exact_match(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_key: str,
    right_key: str,
    match_type: str,
) -> pd.DataFrame:
    matched = left_df.merge(
        right_df[[right_key, "inci_name"]].rename(columns={right_key: left_key}),
        on=left_key,
        how="left",
    )
    matched["match_type"] = matched["inci_name"].notna().map(
        lambda x: match_type if x else None
    )
    return matched


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
    """
    unmatched_df에 대해 fuzzy 수행 후
    - auto matched
    - review candidates
    - still unmatched
    로 분리
    """
    work = unmatched_df.copy()

    cosing_map = (
        cosing_df[[source_key_col, "inci_name"]]
        .dropna(subset=[source_key_col])
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

    #1 자동 승인 매칭
    auto = work[work["fuzzy_key_auto"].notna()].copy()
    auto = auto.merge(
        cosing_map.rename(
            columns={
                source_key_col: "fuzzy_key_auto",
                "inci_name": "matched_inci_name"
            }
        ),
        on="fuzzy_key_auto",
        how="left",
    )
    auto["inci_name"] = auto["matched_inci_name"]
    auto["match_type"] = "fuzzy_auto"

    #2 review 후보
    review = work[
        work["fuzzy_key_auto"].isna() & work["fuzzy_key_review"].notna()
    ].copy()
    review = review.merge(
        cosing_map.rename(
            columns={
                source_key_col: "fuzzy_key_review",
                "inci_name": "review_inci_name",
            }
        ),
        on="fuzzy_key_review",
        how="left",
    )

    #3 최종 unmatched
    still_unmatched = work[
        work["fuzzy_key_auto"].isna() & work["fuzzy_key_review"].isna()
    ].copy()

    return auto, review, still_unmatched