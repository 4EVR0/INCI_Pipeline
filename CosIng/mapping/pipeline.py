import pandas as pd

from .io import load_kcia_csv, load_cosing_csv
from .normalizer import build_name_keys
from .matcher import deduplicate_cosing, exact_match, fuzzy_match_dataframe


class KCIACosIngMapper:
    def __init__(
        self,
        kcia_path,
        cosing_path,
        fuzzy_auto_threshold=95,
        fuzzy_review_threshold=90,
    ):
        self.kcia_path = kcia_path
        self.cosing_path = cosing_path
        self.fuzzy_auto_threshold = fuzzy_auto_threshold
        self.fuzzy_review_threshold = fuzzy_review_threshold

        self.kcia = None
        self.cosing = None

    def load(self):
        self.kcia = load_kcia_csv(self.kcia_path)
        self.cosing = load_cosing_csv(self.cosing_path)

        self.kcia = self.kcia.copy()
        self.cosing = self.cosing.copy()

        self.kcia["name_raw"] = self.kcia["std_name_en"]
        self.cosing["name_raw"] = self.cosing["inci_name"]

        kcia_keys = build_name_keys(self.kcia["name_raw"])
        cosing_keys = build_name_keys(self.cosing["name_raw"])

        self.kcia = pd.concat([self.kcia, kcia_keys], axis=1)
        self.cosing = pd.concat([self.cosing, cosing_keys], axis=1)

    def run(self):
        if self.kcia is None or self.cosing is None:
            self.load()

        cosing_basic = deduplicate_cosing(self.cosing, "key_basic")
        cosing_full = deduplicate_cosing(self.cosing, "key_full")
        cosing_sorted = deduplicate_cosing(self.cosing, "key_sorted")
        cosing_paren_removed = deduplicate_cosing(self.cosing, "key_paren_removed")
        cosing_sorted_strict = deduplicate_cosing(self.cosing, "key_sorted_strict")

        # Step 1: basic exact
        step1 = exact_match(
            left_df=self.kcia,
            right_df=cosing_basic,
            left_key="key_basic",
            right_key="key_basic",
            match_type="exact_basic",
        )
        matched1 = step1[step1["inci_name"].notna()].copy()
        unmatched1 = step1[step1["inci_name"].isna()].copy()

        # Step 2: normalized exact
        step2_input = unmatched1.drop(columns=["inci_name", "match_type"])
        step2 = exact_match(
            left_df=step2_input,
            right_df=cosing_full,
            left_key="key_full",
            right_key="key_full",
            match_type="exact_full_normalized",
        )
        matched2 = step2[step2["inci_name"].notna()].copy()
        unmatched2 = step2[step2["inci_name"].isna()].copy()

        # Step 3: parentheses/special-mark removed exact
        step3_input = unmatched2.drop(columns=["inci_name", "match_type"])
        step3 = exact_match(
            left_df=step3_input,
            right_df=cosing_paren_removed,
            left_key="key_paren_removed",
            right_key="key_paren_removed",
            match_type="exact_paren_removed",
        )
        matched3 = step3[step3["inci_name"].notna()].copy()
        unmatched3 = step3[step3["inci_name"].isna()].copy()

        # Step 4: word sorted exact
        step4_input = unmatched3.drop(columns=["inci_name", "match_type"])
        step4 = exact_match(
            left_df=step4_input,
            right_df=cosing_sorted,
            left_key="key_sorted",
            right_key="key_sorted",
            match_type="exact_word_sorted",
        )
        matched4 = step4[step4["inci_name"].notna()].copy()
        unmatched4 = step4[step4["inci_name"].isna()].copy()

        # Step 5: strict word sorted exact
        step5_input = unmatched4.drop(columns=["inci_name", "match_type"])
        step5 = exact_match(
            left_df=step5_input,
            right_df=cosing_sorted_strict,
            left_key="key_sorted_strict",
            right_key="key_sorted_strict",
            match_type="exact_word_sorted_strict",
        )
        matched5 = step5[step5["inci_name"].notna()].copy()
        unmatched5 = step5[step5["inci_name"].isna()].copy()

        # Step 6: fuzzy
        step6_input = unmatched5.drop(columns=["inci_name", "match_type"])
        fuzzy_auto, fuzzy_review, final_unmatched = fuzzy_match_dataframe(
            unmatched_df=step6_input,
            cosing_df=cosing_full,
            source_key_col="key_full",
            auto_threshold=self.fuzzy_auto_threshold,
            review_threshold=self.fuzzy_review_threshold,
        )

        final_matched = pd.concat(
            [matched1, matched2, matched3, matched4, matched5, fuzzy_auto],
            ignore_index=True,
        )

        return {
            "kcia": self.kcia,
            "cosing": self.cosing,
            "matched_final": final_matched,
            "fuzzy_review": fuzzy_review,
            "final_unmatched": final_unmatched,
            "matched_step1": matched1,
            "matched_step2": matched2,
            "matched_step3": matched3,
            "matched_step4": matched4,
            "matched_step5": matched5,
            "matched_fuzzy_auto": fuzzy_auto,
        }

    @staticmethod
    def build_summary(results: dict) -> pd.DataFrame:
        matched_final = results["matched_final"]
        fuzzy_review = results["fuzzy_review"]
        final_unmatched = results["final_unmatched"]
        kcia = results["kcia"]
        cosing = results["cosing"]

        summary = pd.DataFrame([{
            "kcia_total": len(kcia),
            "cosing_total": len(cosing),
            "matched_final_total": len(matched_final),
            "unmatched_kcia_total": len(final_unmatched),
            "fuzzy_review_total": len(fuzzy_review),
            "match_rate_vs_kcia": round(len(matched_final) / len(kcia) * 100, 2),
        }])

        return summary