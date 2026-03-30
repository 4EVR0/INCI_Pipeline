from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from .config import Settings
from .io import (
    load_cosing_csv,
    load_kcia_csv,
    locate_inputs,
    normalize_output_nulls,
    write_csv,
)
from .matcher import deduplicate_cosing, exact_match, fuzzy_match_dataframe
from .normalizer import build_name_keys, normalize_cas


GRAPH_RAG_COLS = [
    "ingredient_code",
    "std_name_ko",
    "std_name_en",
    "old_name_ko",
    "kcia_cas_no",
    "canonical_inci_name",
    "cosing_substance_id",
    "cosing_cas_no",
    "function_names",
    "cosmetic_restriction",
    "other_restrictions",
    "identified_ingredient",
    "status",
    "match_type",
    "match_score",
]


class KCIACosIngSilverMapper:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.kcia: Optional[pd.DataFrame] = None
        self.cosing: Optional[pd.DataFrame] = None
        self.input_meta: dict[str, str] = {}

    def load(self):
        kcia_input, cosing_input = locate_inputs(self.settings)
        self.input_meta = {
            "kcia_source_mode": kcia_input.source,
            "kcia_input_path": str(kcia_input.path),
            "kcia_s3_key": kcia_input.s3_key or "",
            "cosing_source_mode": cosing_input.source,
            "cosing_input_path": str(cosing_input.path),
            "cosing_s3_key": cosing_input.s3_key or "",
        }

        self.kcia = load_kcia_csv(kcia_input.path)
        self.cosing = load_cosing_csv(cosing_input.path)

        self.kcia = self.kcia.copy()
        self.cosing = self.cosing.copy()

        self.kcia["name_raw"] = self.kcia["std_name_en"]
        self.cosing["name_raw"] = self.cosing["inci_name"]

        kcia_keys = build_name_keys(self.kcia["name_raw"])
        cosing_keys = build_name_keys(self.cosing["name_raw"])

        self.kcia = pd.concat([self.kcia, kcia_keys], axis=1)
        self.cosing = pd.concat([self.cosing, cosing_keys], axis=1)

        self.kcia["key_cas"] = self.kcia["cas_no"].apply(normalize_cas)
        self.cosing["key_cas"] = self.cosing["cas_no"].apply(normalize_cas)

    def run(self) -> dict[str, pd.DataFrame]:
        if self.kcia is None or self.cosing is None:
            self.load()

        assert self.kcia is not None
        assert self.cosing is not None

        cosing_cas = deduplicate_cosing(self.cosing, "key_cas")
        cosing_basic = deduplicate_cosing(self.cosing, "key_basic")
        cosing_full = deduplicate_cosing(self.cosing, "key_full")
        cosing_paren_removed = deduplicate_cosing(self.cosing, "key_paren_removed")
        cosing_sorted = deduplicate_cosing(self.cosing, "key_sorted")
        cosing_sorted_strict = deduplicate_cosing(self.cosing, "key_sorted_strict")

        base_drop_cols = [
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
            "match_type",
            "match_score",
            "review_decision",
            "review_reason",
            "candidate_score",
            "candidate_inci_name",
        ]

        cur = self.kcia.copy()
        matched_frames: list[pd.DataFrame] = []
        review_frames: list[pd.DataFrame] = []

        for left_key, right_key, match_type, right_df in [
            ("key_cas", "key_cas", "exact_cas", cosing_cas),
            ("key_basic", "key_basic", "exact_basic", cosing_basic),
            ("key_full", "key_full", "exact_full_normalized", cosing_full),
            ("key_paren_removed", "key_paren_removed", "exact_paren_removed", cosing_paren_removed),
            ("key_sorted", "key_sorted", "exact_word_sorted", cosing_sorted),
            ("key_sorted_strict", "key_sorted_strict", "exact_word_sorted_strict", cosing_sorted_strict),
        ]:
            cur = cur.drop(columns=[c for c in base_drop_cols if c in cur.columns], errors="ignore")
            matched, review_conflict, cur = exact_match(cur, right_df, left_key, right_key, match_type)
            matched_frames.append(matched)
            if not review_conflict.empty:
                review_frames.append(review_conflict)

        fuzzy_auto, fuzzy_review, final_unmatched = fuzzy_match_dataframe(
            unmatched_df=cur,
            cosing_df=cosing_full,
            source_key_col="key_full",
            auto_threshold=self.settings.fuzzy_auto_threshold,
            review_threshold=self.settings.fuzzy_review_threshold,
        )

        matched_final = pd.concat([*matched_frames, fuzzy_auto], ignore_index=True)
        fuzzy_review_all = pd.concat([*review_frames, fuzzy_review], ignore_index=True)

        matched_final = self._standardize_final_matched(matched_final)
        fuzzy_review_all = self._standardize_fuzzy_review(fuzzy_review_all)
        final_unmatched = self._standardize_unmatched(final_unmatched)
        graphrag_map = self._build_graphrag_map(matched_final)

        return {
            "matched_final": normalize_output_nulls(matched_final),
            "fuzzy_review": normalize_output_nulls(fuzzy_review_all),
            "final_unmatched": normalize_output_nulls(final_unmatched),
            "graphrag_map": normalize_output_nulls(graphrag_map),
            "mapping_summary": self.build_summary(matched_final, fuzzy_review_all, final_unmatched),
        }

    def _standardize_final_matched(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        rename_map = {
            "cas_no": "kcia_cas_no",
            "substance_id": "cosing_substance_id",
        }
        out = out.rename(columns=rename_map)
        out["canonical_inci_name"] = out["inci_name"]
        keep_cols = [
            "ingredient_code",
            "std_name_ko",
            "std_name_en",
            "old_name_ko",
            "kcia_cas_no",
            "canonical_inci_name",
            "cosing_substance_id",
            "cosing_cas_no",
            "function_names",
            "cosmetic_restriction",
            "other_restrictions",
            "identified_ingredient",
            "status",
            "match_type",
            "match_score",
            "kcia_source",
            "kcia_ingest_date",
            "kcia_batch_id",
            "cosing_source",
            "cosing_ingest_date",
            "cosing_batch_id",
            "as_of_date",
        ]
        for col in keep_cols:
            if col not in out.columns:
                out[col] = ""
        return out[keep_cols].copy()

    def _standardize_fuzzy_review(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out = out.rename(columns={"cas_no": "kcia_cas_no", "substance_id": "cosing_substance_id"})

        if "candidate_inci_name" not in out.columns:
            out["candidate_inci_name"] = out.get("inci_name", "")

        keep_cols = [
            "ingredient_code",
            "std_name_ko",
            "std_name_en",
            "old_name_ko",
            "kcia_cas_no",
            "candidate_inci_name",
            "cosing_substance_id",
            "cosing_cas_no",
            "candidate_score",
            "function_names",
            "cosmetic_restriction",
            "other_restrictions",
            "identified_ingredient",
            "status",
            "review_decision",
            "review_reason",
            "fuzzy_key_review",
        ]
        for col in keep_cols:
            if col not in out.columns:
                out[col] = ""
        return out[keep_cols].copy()

    def _standardize_unmatched(self, df: pd.DataFrame) -> pd.DataFrame:
        keep_cols = [
            "ingredient_code",
            "std_name_ko",
            "std_name_en",
            "old_name_ko",
            "cas_no",
            "as_of_date",
        ]
        out = df.copy()
        for col in keep_cols:
            if col not in out.columns:
                out[col] = ""
        return out[keep_cols].rename(columns={"cas_no": "kcia_cas_no"})

    def _build_graphrag_map(self, matched_final: pd.DataFrame) -> pd.DataFrame:
        out = matched_final.copy()
        return out[GRAPH_RAG_COLS].copy()

    def build_summary(
        self,
        matched_final: pd.DataFrame,
        fuzzy_review: pd.DataFrame,
        final_unmatched: pd.DataFrame,
    ) -> pd.DataFrame:
        assert self.kcia is not None
        assert self.cosing is not None

        kcia_total = len(self.kcia)
        cosing_total = len(self.cosing)
        matched_total = len(matched_final)
        unmatched_total = len(final_unmatched)
        review_total = len(fuzzy_review)

        accounted_total = matched_total + unmatched_total + review_total
        missing_total = kcia_total - accounted_total

        summary = pd.DataFrame(
            [
                {
                    **self.input_meta,
                    "kcia_total": kcia_total,
                    "cosing_total": cosing_total,
                    "matched_final_total": matched_total,
                    "unmatched_kcia_total": unmatched_total,
                    "fuzzy_review_total": review_total,
                    "accounted_total": accounted_total,
                    "missing_total": missing_total,
                    "match_rate_vs_kcia": round(matched_total / kcia_total * 100, 2) if kcia_total else 0.0,
                    "coverage_excluding_review": round((matched_total + unmatched_total) / kcia_total * 100, 2) if kcia_total else 0.0,
                    "coverage_including_review": round(accounted_total / kcia_total * 100, 2) if kcia_total else 0.0,
                    "fuzzy_auto_threshold": self.settings.fuzzy_auto_threshold,
                    "fuzzy_review_threshold": self.settings.fuzzy_review_threshold,
                }
            ]
        )
        return summary
    

def run_and_save(settings: Settings) -> dict[str, Path]:
    mapper = KCIACosIngSilverMapper(settings)
    results = mapper.run()

    output_paths = {
        "matched_final": settings.output_dir / "kcia_cosing_matched_final.csv",
        "graphrag_map": settings.output_dir / "kcia_cosing_graphrag_map.csv",
        "fuzzy_review": settings.output_dir / "kcia_cosing_fuzzy_review_latest.csv",
        "final_unmatched": settings.output_dir / "kcia_cosing_unmatched_final.csv",
        "mapping_summary": settings.output_dir / "mapping_summary.csv",
    }

    for key, path in output_paths.items():
        write_csv(results[key], path)

    return output_paths