"""성분사전 추출 항목 → 근거 후보 CSV (그래프 적재 전 단계).

책 원문이 들어 있으므로 입력·출력 모두 git에서 제외되는 경로(dev_data/, data/)에만 둔다.

실행:
  python -m pipeline.reference_book.run \\
    --entries "dev_data/dictionary-poc/entries_part1_*.jsonl" \\
    --gold <kcia_cosing_gold_ingredients.csv>

출력: data/gold/reference_book/run_id=<id>/
  entries_summary.csv          항목별 매칭 상태·효능·주의 문구로 막힌 효능
  reference_book_evidence.csv  (inci_name, effect_code) 근거 후보 — 그래프 적재 입력 예정
  reference_book_review.csv    후보 여러 개·미매칭(사람이 build.MANUAL_INCI로 확정)
  metadata.json
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common.metadata import write_json
from common.paths import PROJECT_ROOT
from pipeline.reference_book.build import BOOK_CITATION, build
from pipeline.reference_book.entries import load_entries


def main() -> None:
    ap = argparse.ArgumentParser(description="성분사전 항목 → reference_book 근거 후보")
    ap.add_argument("--entries", required=True, help="추출 JSONL glob")
    ap.add_argument("--gold", type=Path, required=True)
    ap.add_argument("--data-root", type=Path, default=PROJECT_ROOT / "data")
    args = ap.parse_args()

    paths = [Path(p) for p in sorted(glob.glob(args.entries))]
    if not paths:
        raise SystemExit(f"입력 없음: {args.entries}")
    entries = load_entries(paths)
    gold = pd.read_csv(args.gold, dtype=str).fillna("")
    summary, evidence, review = build(entries, gold)

    run_id = f"reference_book_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    out = args.data_root / "gold" / "reference_book" / f"run_id={run_id}"
    out.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out / "entries_summary.csv", index=False, encoding="utf-8-sig")
    evidence.to_csv(out / "reference_book_evidence.csv", index=False, encoding="utf-8-sig")
    review.to_csv(out / "reference_book_review.csv", index=False, encoding="utf-8-sig")
    meta = {
        "run_id": run_id, "citation": BOOK_CITATION,
        "inputs": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
        "gold": {"file": args.gold.name, "sha256": hashlib.sha256(args.gold.read_bytes()).hexdigest()},
        "entries": len(summary),
        "print_pages": sorted({int(p) for p in summary["print_page"]}),
        "by_scope": dict(Counter(summary["claim_scope"])),
        "by_match_status": dict(Counter(summary["match_status"])),
        "evidence_rows": len(evidence),
        "evidence_by_effect": dict(Counter(evidence["effect_code"])) if len(evidence) else {},
        "blocked_by_caution": int((summary["blocked_by_caution"] != "").sum()),
        "review_rows": len(review),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_json(out / "metadata.json", meta)
    print(json.dumps({k: v for k, v in meta.items() if k != "inputs"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
