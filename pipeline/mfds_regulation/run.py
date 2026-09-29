"""식약처 사용제한 원료정보 월간 배치: Bronze → Silver → Gold.

출력 (data/ 는 gitignored):
  data/bronze/mfds_regulation/run_id=<id>/rstrc_raw.json, regl_raw.json, metadata.json
  data/silver/mfds_regulation/run_id=<id>/kr_regulation_classified.csv
  data/gold/mfds_regulation/run_id=<id>/ingredient_kr_regulation.csv   ← Neo4j 적재 입력
                                        kr_regulation_review.csv       ← CAS/이명 단독 일치(자동 적용 안 함)
                                        kr_regulation_unmatched.csv    ← 어떤 성분에도 안 걸린 한국 banned 행
                                        metadata.json

실행:
  python -m pipeline.mfds_regulation.run                     # API 수집부터 (REGULATION_API_KEY 필요)
  python -m pipeline.mfds_regulation.run \
      --raw output/regulation_probe/rstrc.json --raw-regl output/regulation_probe/regl.json  # 수집 생략
  옵션: --gold <kcia_cosing_gold_ingredients.csv> (기본: data/gold 최신)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from common.metadata import write_json
from common.paths import PROJECT_ROOT
from pipeline.mfds_regulation.collect import fetch_all
from pipeline.mfds_regulation.transform import build_silver, match_gold

SOURCE = "mfds_regulation"
GOLD_INCI_NAME = "kcia_cosing_gold_ingredients.csv"


def _latest_gold_csv(data_root: Path) -> Path:
    candidates = sorted((data_root / "gold").glob(f"run_id=*/{GOLD_INCI_NAME}"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"{GOLD_INCI_NAME}를 찾지 못했습니다: {data_root / 'gold'} (--gold 지정)")
    return candidates[0]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(*, raw_path: Path | None, regl_path: Path | None, gold_path: Path | None,
        data_root: Path) -> dict:
    run_id = f"{SOURCE}_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    bronze_dir = data_root / "bronze" / SOURCE / f"run_id={run_id}"
    silver_dir = data_root / "silver" / SOURCE / f"run_id={run_id}"
    gold_dir = data_root / "gold" / SOURCE / f"run_id={run_id}"
    for d in (bronze_dir, silver_dir, gold_dir):
        d.mkdir(parents=True, exist_ok=True)

    # Bronze
    raw: dict[str, list[dict]] = {}
    bronze_meta: dict = {"source": SOURCE, "layer": "bronze", "run_id": run_id, "endpoints": {}}
    for endpoint, local in (("rstrc", raw_path), ("regl", regl_path)):
        out = bronze_dir / f"{endpoint}_raw.json"
        if local:
            shutil.copyfile(local, out)
            raw[endpoint] = json.loads(out.read_text(encoding="utf-8"))
            meta = {"mode": "from_file", "source_file": str(local), "fetched_rows": len(raw[endpoint])}
        else:
            load_dotenv(PROJECT_ROOT / ".env")
            raw[endpoint], meta = fetch_all(os.getenv("REGULATION_API_KEY", ""), endpoint)
            meta["mode"] = "api"
            out.write_text(json.dumps(raw[endpoint], ensure_ascii=False), encoding="utf-8")
        meta["sha256"] = _sha256(out)
        bronze_meta["endpoints"][endpoint] = meta
    write_json(bronze_dir / "metadata.json", bronze_meta)
    raw_rows = raw["rstrc"]

    # Silver
    silver = build_silver(raw_rows, raw["regl"])
    silver.to_csv(silver_dir / "kr_regulation_classified.csv", index=False, encoding="utf-8-sig")

    # Gold
    gold_path = gold_path or _latest_gold_csv(data_root)
    ingredients = pd.read_csv(gold_path, dtype=str).fillna("")
    inputs = {"gold_csv": str(gold_path), "gold_sha256": _sha256(gold_path)}
    matched, review = match_gold(silver, ingredients)
    used = {rid for ids in matched["reg_ids"] for rid in ids.split("|")}
    unmatched = silver[(silver["kr_reg_status"] == "banned") & ~silver["reg_id"].isin(used)]

    matched.to_csv(gold_dir / "ingredient_kr_regulation.csv", index=False, encoding="utf-8-sig")
    review.to_csv(gold_dir / "kr_regulation_review.csv", index=False, encoding="utf-8-sig")
    unmatched.to_csv(gold_dir / "kr_regulation_unmatched.csv", index=False, encoding="utf-8-sig")

    summary = {
        "source": SOURCE, "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_rows": len(raw_rows),
        "korea_rows": len(silver),
        "korea_by_status": silver["kr_reg_status"].value_counts().to_dict(),
        "matched_ingredients": len(matched),
        # gold: KCIA/CosIng 성분, mfds_name: 규제 행 영문명으로 만든 후보 INCI명(그래프에 있을 때만 적재됨)
        "matched_by_source_status": {
            source: group["kr_reg_status"].value_counts().to_dict()
            for source, group in matched.groupby("source")
        },
        "gold_banned_ingredients": sorted(matched.loc[(matched["source"] == "gold")
                                                      & (matched["kr_reg_status"] == "banned"), "inci_name"]),
        "review_rows": len(review),
        "unmatched_banned_rows": len(unmatched),
        "inputs": inputs,
    }
    write_json(gold_dir / "metadata.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="식약처 사용제한 원료정보 → 국내 규제 상태 배치")
    parser.add_argument("--raw", type=Path, help="이미 수집한 rstrc JSON (API 호출 생략)")
    parser.add_argument("--raw-regl", type=Path, help="이미 수집한 regl JSON (API 호출 생략)")
    parser.add_argument("--gold", type=Path, help=f"INCI Gold CSV (기본: data/gold 최신 {GOLD_INCI_NAME})")
    parser.add_argument("--data-root", type=Path, default=PROJECT_ROOT / "data")
    args = parser.parse_args()
    summary = run(raw_path=args.raw, regl_path=args.raw_regl, gold_path=args.gold,
                  data_root=args.data_root)
    print(json.dumps({k: v for k, v in summary.items() if k != "inputs"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
