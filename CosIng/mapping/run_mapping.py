from pathlib import Path
import pandas as pd

from mapping.config import KCIA_CSV_PATH, COSING_CSV_PATH, OUTPUT_DIR
from mapping.mapper import KCIACosIngMapper
from mapping.fuzzy_mapper import KCIACosIngFuzzyMapper


def main():
    mapper = KCIACosIngMapper(
        kcia_csv_path=Path(KCIA_CSV_PATH),
        cosing_csv_path=Path(COSING_CSV_PATH),
    )

    artifacts = mapper.run_exact_mapping()

    fuzzy_mapper = KCIACosIngFuzzyMapper(
        unmatched_kcia=artifacts.unmatched_kcia,
        cosing=artifacts.cosing,
    )

    fuzzy_matched, fuzzy_review = fuzzy_mapper.run()

    final_matched = pd.concat(
        [artifacts.matched_exact, fuzzy_matched],
        ignore_index=True
    )

    matched_kcia_names = set(final_matched["_kcia_name_en"].astype(str))
    final_unmatched_kcia = artifacts.kcia[
        ~artifacts.kcia["_kcia_name_en"].astype(str).isin(matched_kcia_names)
    ].copy()

    matched_cosing_names = set(final_matched["_cosing_inci_name"].dropna().astype(str))
    cosing_only = artifacts.cosing[
        ~artifacts.cosing["_cosing_inci_name"].astype(str).isin(matched_cosing_names)
    ].copy()

    summary = pd.DataFrame([
        {
            "kcia_total": len(artifacts.kcia),
            "cosing_total": len(artifacts.cosing),
            "matched_exact_total": len(artifacts.matched_exact),
            "matched_fuzzy_total": len(fuzzy_matched),
            "matched_final_total": len(final_matched),
            "unmatched_kcia_total": len(final_unmatched_kcia),
            "cosing_only_total": len(cosing_only),
            "fuzzy_review_total": len(fuzzy_review),
            "match_rate_vs_kcia": round(len(final_matched) / len(artifacts.kcia) * 100, 2) if len(artifacts.kcia) > 0 else 0.0,
        }
    ])

    matched_exact_path = OUTPUT_DIR / "kcia_cosing_matched_exact.csv"
    matched_fuzzy_path = OUTPUT_DIR / "kcia_cosing_matched_fuzzy.csv"
    matched_final_path = OUTPUT_DIR / "kcia_cosing_matched_final.csv"
    unmatched_kcia_path = OUTPUT_DIR / "kcia_unmatched_final.csv"
    cosing_only_path = OUTPUT_DIR / "cosing_only_final.csv"
    fuzzy_review_path = OUTPUT_DIR / "fuzzy_review.csv"
    summary_path = OUTPUT_DIR / "mapping_summary_v2.csv"

    artifacts.matched_exact.to_csv(matched_exact_path, index=False, encoding="utf-8-sig")
    fuzzy_matched.to_csv(matched_fuzzy_path, index=False, encoding="utf-8-sig")
    final_matched.to_csv(matched_final_path, index=False, encoding="utf-8-sig")
    final_unmatched_kcia.to_csv(unmatched_kcia_path, index=False, encoding="utf-8-sig")
    cosing_only.to_csv(cosing_only_path, index=False, encoding="utf-8-sig")
    fuzzy_review.to_csv(fuzzy_review_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    print("\n=== Mapping v2 Done ===")
    print(summary.to_string(index=False))
    print(f"\nmatched_exact: {matched_exact_path}")
    print(f"matched_fuzzy: {matched_fuzzy_path}")
    print(f"matched_final: {matched_final_path}")
    print(f"unmatched_final: {unmatched_kcia_path}")
    print(f"cosing_only_final: {cosing_only_path}")
    print(f"fuzzy_review: {fuzzy_review_path}")
    print(f"summary: {summary_path}")


if __name__ == "__main__":
    main()