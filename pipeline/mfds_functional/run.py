"""식약처 기능성화장품 고시 원료 목록 생성 (고시 개정 시 수동 실행).

입력 PDF는 저장소에 넣지 않는다. 결과 CSV와 출처(파일 SHA-256, 고시 번호)만 커밋한다.

실행:
  python -m pipeline.mfds_functional.run \\
    --standard-pdf "기능성화장품 기준 및 시험방법 개정고시(전문).pdf" \\
    --annex4-pdf "[별표 4] 자료제출이 생략되는 기능성화장품의 종류.pdf" \\
    --gold dev_data/.../kcia_cosing_gold_ingredients.csv \\
    --mfds-snapshot dev_data/mfds-pilot/full-20260926/mfds_ingredients.json

PDF 텍스트 추출에 poppler의 pdftotext가 필요하다(macOS: brew install poppler).
결과: config/mfds_functional_ingredients.csv, config/mfds_functional_ingredients.meta.json
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common.paths import PROJECT_ROOT
from pipeline.mfds_functional.extract import merge, parse_annex4, parse_standard
from pipeline.mfds_functional.match import build_index, match

OUT_CSV = PROJECT_ROOT / "config" / "mfds_functional_ingredients.csv"
COLUMNS = ["kor_name", "function", "inci_name", "match_status", "match_source", "candidates", "aliases",
           "max_content", "condition", "effect_codes", "sources", "review_note"]


def _pdf_text(path: Path) -> str:
    return subprocess.run(["pdftotext", "-layout", str(path), "-"], check=True,
                          capture_output=True, text=True).stdout


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description="식약처 기능성화장품 고시 원료 목록 생성")
    ap.add_argument("--standard-pdf", type=Path, required=True, help="「기능성화장품 기준 및 시험방법」 전문 PDF")
    ap.add_argument("--annex4-pdf", type=Path, required=True, help="「기능성화장품 심사에 관한 규정」 [별표 4] PDF")
    ap.add_argument("--gold", type=Path, required=True, help="KCIA/CosIng Gold CSV")
    ap.add_argument("--mfds-snapshot", type=Path, required=True, help="식약처 원료성분 API 스냅샷 JSON")
    ap.add_argument("--out", type=Path, default=OUT_CSV)
    args = ap.parse_args()

    standard_text = _pdf_text(args.standard_pdf)
    annex4_text = _pdf_text(args.annex4_pdf)
    notice = re.findall(r"고시\s*제(\d{4}\s*-\s*\d+)호\s*\(([\d.\s]+),\s*개정\)", standard_text)
    items = merge(parse_standard(standard_text), parse_annex4(annex4_text))
    gold = pd.read_csv(args.gold, dtype=str).fillna("")
    mfds_rows = json.loads(args.mfds_snapshot.read_text(encoding="utf-8"))
    rows = match(items, build_index(gold, mfds_rows))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "standard": {"title": "기능성화장품 기준 및 시험방법", "file": args.standard_pdf.name,
                         "sha256": _sha256(args.standard_pdf),
                         "latest_notice": (f"식품의약품안전처 고시 제{re.sub(r'\\s', '', notice[-1][0])}호 "
                                           f"({notice[-1][1].strip()})") if notice else None,
                         "appendices": "별표2 미백, 별표3 주름개선, 별표4 자외선차단, 별표8 여드름, 별표9 탈모"},
            "annex4": {"title": "기능성화장품 심사에 관한 규정 [별표 4] 자료제출이 생략되는 기능성화장품의 종류",
                       "file": args.annex4_pdf.name, "sha256": _sha256(args.annex4_pdf),
                       "sections": "1 자외선차단, 2 미백, 3 주름개선, 6 여드름"},
            "gold": {"file": args.gold.name, "sha256": _sha256(args.gold)},
            "mfds_snapshot": {"file": str(args.mfds_snapshot.name), "sha256": _sha256(args.mfds_snapshot)},
        },
        "counts": {
            "ingredients": len(rows),
            "by_function": dict(Counter(r["function"] for r in rows)),
            "by_match_status": dict(Counter(r["match_status"] for r in rows)),
        },
    }
    meta_path = args.out.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(meta["counts"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
