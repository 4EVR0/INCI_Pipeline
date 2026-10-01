"""성분사전 Bronze → Silver → Gold (수동 실행, 월간 DAG 밖).

책은 바뀌지 않는 자료라 추출을 추가하거나 검토 결과(silver.MANUAL_INCI)를 반영할 때만 실행한다.
입력(dev_data/)·로컬 출력(data/)은 git에서 제외된다. 그래프 적재는 아직 하지 않는다.

  bronze  추출 JSONL 검증 후 그대로 저장            data/bronze/reference_book/run_id=…/entries.jsonl   (로컬만: 설명 원문 포함)
  silver  최신 bronze + KCIA/CosIng Gold 매칭·규칙   data/silver/reference_book/run_id=…/{matched,review,unmapped}.csv
          → s3://$S3_BUCKET/INCI_data_silver/reference_book/run_id=…/
  gold    최신 silver matched → 성분×효능 근거        data/gold/reference_book/run_id=…/reference_book_evidence.csv
          → s3://$S3_BUCKET/INCI_data_gold/reference_book/run_id=…/

원문 정책: 설명 전문은 Bronze(로컬)에만. Silver는 효능 주장 구절만(검토용), Gold는 구조화된 값만.

실행:
  python -m pipeline.reference_book.run --stage all --entries "dev_data/dictionary-poc/entries_part1_*.jsonl"
  python -m pipeline.reference_book.run --stage silver   # 검토 반영 후: 최신 bronze부터 다시
  옵션: --gold <kcia_cosing_gold_ingredients.csv> (기본: data/gold 최신), --data-root, --no-upload(S3 생략)
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common.metadata import write_json
from common.paths import PROJECT_ROOT
from pipeline.reference_book.bronze import load_entries
from pipeline.reference_book.gold import BOOK_CITATION, build_gold
from pipeline.reference_book.silver import build_silver

SOURCE = "reference_book"
KCIA_GOLD_NAME = "kcia_cosing_gold_ingredients.csv"
# oliveyoung_common.s3_paths의 INCI 접두어 규칙(INCI_data_silver/…, INCI_data_gold/…)을 따른다.
S3_PREFIXES = {
    "silver": os.getenv("REFERENCE_BOOK_S3_SILVER_PREFIX", "INCI_data_silver/reference_book"),
    "gold": os.getenv("REFERENCE_BOOK_S3_GOLD_PREFIX", "INCI_data_gold/reference_book"),
}
DEFAULT_BUCKET = "oliveyoung-crawl-data"


def _run_dir(data_root: Path, layer: str, run_id: str) -> Path:
    path = data_root / layer / SOURCE / f"run_id={run_id}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _latest(data_root: Path, layer: str, file_name: str) -> Path:
    found = sorted((data_root / layer / SOURCE).glob(f"run_id=*/{file_name}"), reverse=True)
    if not found:
        raise FileNotFoundError(f"{layer} 결과가 없습니다: {data_root / layer / SOURCE} (앞 단계를 먼저 실행)")
    return found[0]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _upload(out: Path, layer: str, run_id: str) -> list[str]:
    """run 폴더의 CSV·metadata를 S3에 올린다. Bronze는 원문이 있어 올리지 않는다."""
    from pipeline.silver_mapping.kcia_cosing.s3_io import upload_file

    bucket = os.getenv("S3_BUCKET", DEFAULT_BUCKET)
    prefix = f"{S3_PREFIXES[layer]}/run_id={run_id}"
    uris = [upload_file(path, bucket, f"{prefix}/{path.name}")
            for path in sorted(out.glob("*.csv")) + [out / "metadata.json"]]
    print(f"[{layer}] S3 업로드 {len(uris)}개 → s3://{bucket}/{prefix}/")
    return uris


def run_bronze(entries_glob: str, data_root: Path, run_id: str) -> Path:
    paths = [Path(p) for p in sorted(glob.glob(entries_glob))]
    if not paths:
        raise SystemExit(f"추출 입력 없음: {entries_glob}")
    entries = load_entries(paths)
    out = _run_dir(data_root, "bronze", run_id)
    with open(out / "entries.jsonl", "w", encoding="utf-8") as f:
        for entry in entries:
            entry["extraction_source"] = entry.pop("_source")
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    write_json(out / "metadata.json", {
        "source": SOURCE, "layer": "bronze", "run_id": run_id, "citation": BOOK_CITATION,
        "inputs": {p.name: _sha256(p) for p in paths},
        "entries": len(entries), "print_pages": sorted({e["print_page"] for e in entries}),
        "by_scope": dict(Counter(e["claim_scope"] for e in entries)),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    print(f"[bronze] {len(entries)}항목 → {out}")
    return out


def run_silver(data_root: Path, run_id: str, gold_csv: Path | None, upload: bool = False) -> Path:
    bronze = _latest(data_root, "bronze", "entries.jsonl")
    gold_csv = gold_csv or sorted((data_root / "gold").glob(f"run_id=*/{KCIA_GOLD_NAME}"), reverse=True)[0]
    entries = load_entries([bronze])
    tables = build_silver(entries, pd.read_csv(gold_csv, dtype=str).fillna(""))
    out = _run_dir(data_root, "silver", run_id)
    for name, table in tables.items():
        table.to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")
    matched = tables["matched"]
    entries_view = pd.concat(tables.values()).drop_duplicates(["print_page", "kor_name"])  # partial 중복 제거
    write_json(out / "metadata.json", {
        "source": SOURCE, "layer": "silver", "run_id": run_id,
        "inputs": {"bronze": str(bronze), "bronze_sha256": _sha256(bronze),
                   "kcia_gold": gold_csv.name, "kcia_gold_sha256": _sha256(gold_csv)},
        "counts": {name: len(table) for name, table in tables.items()},
        "by_match_status": dict(Counter(entries_view["match_status"])),
        "blocked_by_caution": int((entries_view["blocked_effects"] != "").sum()),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    print(f"[silver] matched {len(matched)} / review {len(tables['review'])} / unmapped {len(tables['unmapped'])} → {out}")
    if upload:
        _upload(out, "silver", run_id)
    return out


def run_gold(data_root: Path, run_id: str, upload: bool = False) -> Path:
    matched_path = _latest(data_root, "silver", "matched.csv")
    evidence = build_gold(pd.read_csv(matched_path, dtype=str).fillna(""))
    out = _run_dir(data_root, "gold", run_id)
    evidence.to_csv(out / "reference_book_evidence.csv", index=False, encoding="utf-8-sig")
    write_json(out / "metadata.json", {
        "source": SOURCE, "layer": "gold", "run_id": run_id, "citation": BOOK_CITATION,
        "inputs": {"silver_matched": str(matched_path), "sha256": _sha256(matched_path)},
        "evidence_rows": len(evidence), "ingredients": int(evidence["inci_name"].nunique()) if len(evidence) else 0,
        "by_effect": dict(Counter(evidence["effect_code"])) if len(evidence) else {},
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    print(f"[gold] 근거 {len(evidence)}건 → {out}")
    if upload:
        _upload(out, "gold", run_id)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="성분사전 Bronze → Silver → Gold")
    ap.add_argument("--stage", choices=["bronze", "silver", "gold", "all"], default="all")
    ap.add_argument("--entries", help="추출 JSONL glob (bronze·all에 필요)")
    ap.add_argument("--gold", type=Path, help=f"KCIA/CosIng Gold CSV (기본: data/gold 최신 {KCIA_GOLD_NAME})")
    ap.add_argument("--data-root", type=Path, default=PROJECT_ROOT / "data")
    ap.add_argument("--no-upload", action="store_true", help="Silver·Gold S3 업로드 생략(Bronze는 항상 로컬만)")
    args = ap.parse_args()
    run_id = f"{SOURCE}_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    if args.stage in ("bronze", "all"):
        if not args.entries:
            raise SystemExit("--entries가 필요합니다")
        run_bronze(args.entries, args.data_root, run_id)
    if args.stage in ("silver", "all"):
        run_silver(args.data_root, run_id, args.gold, upload=not args.no_upload)
    if args.stage in ("gold", "all"):
        run_gold(args.data_root, run_id, upload=not args.no_upload)


if __name__ == "__main__":
    main()
