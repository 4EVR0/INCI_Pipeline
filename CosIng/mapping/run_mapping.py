from .config import (
    KCIA_FILE,
    COSING_FILE,
    OUTPUT_DIR,
    FUZZY_AUTO_THRESHOLD,
    FUZZY_REVIEW_THRESHOLD,
)
from .pipeline import KCIACosIngMapper


def main():
    mapper = KCIACosIngMapper(
        kcia_path=KCIA_FILE,
        cosing_path=COSING_FILE,
        fuzzy_auto_threshold=FUZZY_AUTO_THRESHOLD,
        fuzzy_review_threshold=FUZZY_REVIEW_THRESHOLD,
    )

    results = mapper.run()
    summary = mapper.build_summary(results)

    results["matched_final"].to_csv(
        OUTPUT_DIR / "matched_final.csv",
        index=False,
        encoding="utf-8-sig",
    )
    results["fuzzy_review"].to_csv(
        OUTPUT_DIR / "fuzzy_review.csv",
        index=False,
        encoding="utf-8-sig",
    )
    results["final_unmatched"].to_csv(
        OUTPUT_DIR / "final_unmatched_kcia.csv",
        index=False,
        encoding="utf-8-sig",
    )
    summary.to_csv(
        OUTPUT_DIR / "mapping_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print("=== Mapping Complete ===")
    print(summary.to_string(index=False))
    print("\nmatch_type counts:")
    print(results["matched_final"]["match_type"].value_counts(dropna=False))


if __name__ == "__main__":
    main()