from __future__ import annotations

import pandas as pd
from rapidfuzz import process, fuzz

from mapping.config import FUZZY_SCORE_THRESHOLD, FUZZY_SCORE_REVIEW_THRESHOLD


class KCIACosIngFuzzyMapper:
    def __init__(
        self,
        unmatched_kcia: pd.DataFrame,
        cosing: pd.DataFrame,
        score_threshold: int = FUZZY_SCORE_THRESHOLD,
        review_threshold: int = FUZZY_SCORE_REVIEW_THRESHOLD,
    ):
        self.unmatched_kcia = unmatched_kcia.copy()
        self.cosing = cosing.copy()
        self.score_threshold = score_threshold
        self.review_threshold = review_threshold

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        cosing_choices = (
            self.cosing[["_cosing_inci_name", "_cosing_inci_name_compact", "_cosing_substance_id", "_cosing_function", "_cosing_cas", "_cosing_ec"]]
            .dropna(subset=["_cosing_inci_name_compact"])
            .drop_duplicates(subset=["_cosing_inci_name_compact"])
            .copy()
        )

        choice_map = dict(
            zip(cosing_choices["_cosing_inci_name_compact"], cosing_choices.to_dict("records"))
        )
        choice_keys = list(choice_map.keys())

        accepted_rows = []
        review_rows = []

        for _, row in self.unmatched_kcia.iterrows():
            query = row.get("_kcia_name_en_compact")

            if not query or pd.isna(query):
                continue

            best = process.extractOne(
                query,
                choice_keys,
                scorer=fuzz.ratio,
            )

            if not best:
                continue

            matched_key, score, _ = best
            cosing_row = choice_map[matched_key]

            merged = row.to_dict()
            merged.update(cosing_row)
            merged["fuzzy_score"] = score

            if score >= self.score_threshold:
                merged["match_type"] = "fuzzy_name"
                accepted_rows.append(merged)
            elif score >= self.review_threshold:
                merged["match_type"] = "fuzzy_review"
                review_rows.append(merged)

        accepted_df = pd.DataFrame(accepted_rows)
        review_df = pd.DataFrame(review_rows)

        return accepted_df, review_df