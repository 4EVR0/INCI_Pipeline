from pathlib import Path

from src.collector import CosIngCollector
from src.splitter import CosIngQuerySplitter


def main():
    log_dir = Path("./logs")
    output_dir = Path("./output")
    cache_path = output_dir / "query_count_cache.json"

    log_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    collector = CosIngCollector(log_dir=log_dir)

    splitter = CosIngQuerySplitter(
        client=collector.client,
        logger=collector.logger,
        cache_path=cache_path,
    )

    print("[1/3] Building queries...")
    queries, oversized = splitter.build_queries()

    print(f"query count: {len(queries)}")
    print(f"oversized count: {len(oversized)}")

    if oversized:
        print("oversized queries:")
        for q, cnt in oversized[:20]:
            print(f"  - {q}: {cnt}")

    if not queries:
        raise RuntimeError("No queries were generated. Check splitter / API response.")

    print("[2/3] Collecting ingredient-only CosIng data...")
    df = collector.collect_by_queries(
        queries=queries,
        save_raw_pages=False,
    )

    print("[3/3] Saving outputs...")
    paths = collector.save_outputs(
        df,
        prefix="cosing_ingredient_only_latest",
    )

    print("\n=== DONE ===")
    print(f"shape: {df.shape}")
    print(f"csv: {paths['csv']}")
    print(f"parquet: {paths['parquet']}")
    print(f"sample: {paths['sample']}")

    if not df.empty:
        print("\nitem_type counts:")
        print(df["item_type"].value_counts(dropna=False))

        print("\nSample rows:")
        cols = ["inci_name", "item_type", "substance_id", "cas_no"]
        existing_cols = [c for c in cols if c in df.columns]
        print(df[existing_cols].head(20).to_string(index=False))

        for keyword in ["RIBOFLAVIN", "NIACINAMIDE", "LEUCINE"]:
            subset = df[df["inci_name"].astype(str).str.upper() == keyword]
            print(f"\n[{keyword}] rows: {len(subset)}")
            if not subset.empty:
                print(subset[existing_cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()