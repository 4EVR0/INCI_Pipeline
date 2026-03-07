import argparse

from config import LOG_DIR, OUTPUT_DIR
from src.collector import CosIngCollector
from src.splitter import CosIngQuerySplitter


def parse_args():
    parser = argparse.ArgumentParser(description="Collect latest CosIng data with stable query splitting")

    parser.add_argument(
        "--save-raw-pages",
        action="store_true",
        help="Save parsed page json files for debugging",
    )

    parser.add_argument(
        "--max-queries",
        type=int,
        default=None,
        help="Limit number of final split queries used for actual row collection",
    )

    parser.add_argument(
        "--count-only",
        action="store_true",
        help="Only build split queries and print them, without collecting rows",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=6,
        help="Max recursive split depth",
    )

    parser.add_argument(
        "--seed-chars",
        type=str,
        default=None,
        help='Override seed chars, e.g. "a" or "abc123"',
    )

    parser.add_argument(
        "--next-chars",
        type=str,
        default=None,
        help='Override next chars, e.g. "abcdefghijklmnopqrstuvwxyz0123456789"',
    )

    parser.add_argument(
        "--max-seeds",
        type=int,
        default=None,
        help="Only use first N seeds after seed-char selection",
    )

    parser.add_argument(
        "--resume-count-cache",
        action="store_true",
        help="Reuse count cache json if exists",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    collector = CosIngCollector(log_dir=LOG_DIR)

    cache_path = OUTPUT_DIR / "count_cache.json" if args.resume_count_cache else None
    seed_chars = list(args.seed_chars) if args.seed_chars else None
    next_chars = list(args.next_chars) if args.next_chars else None

    splitter = CosIngQuerySplitter(
        client=collector.client,
        logger=collector.logger,
        cache_path=cache_path,
        seed_chars=seed_chars,
        next_chars=next_chars,
    )

    queries, oversized_queries = splitter.build_queries(
        max_depth=args.max_depth,
        max_seeds=args.max_seeds,
    )

    queries_path = OUTPUT_DIR / "final_queries.txt"
    with open(queries_path, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(q + "\n")

    oversized_path = OUTPUT_DIR / "oversized_queries.txt"
    with open(oversized_path, "w", encoding="utf-8") as f:
        for q, cnt in oversized_queries:
            f.write(f"{q}\t{cnt}\n")

    print("\n=== Query Split Done ===")
    print(f"Final query count: {len(queries)}")
    print(f"Oversized query count: {len(oversized_queries)}")
    print(f"Preview queries: {queries[:20]}")
    print(f"Saved query list: {queries_path}")
    print(f"Saved oversized list: {oversized_path}")

    if args.count_only:
        return

    if oversized_queries:
        print("\nOversized queries remain. Resolve them before full collection.")
        print("Run in smaller seed groups first, or increase max_depth carefully.")
        return

    df = collector.collect_by_queries(
        queries=queries,
        save_raw_pages=args.save_raw_pages,
        max_queries=args.max_queries,
    )

    paths = collector.save_outputs(df)

    print("\n=== Done ===")
    print(f"Rows collected: {len(df)}")
    for k, v in paths.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()