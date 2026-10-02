"""참고 도서 근거(Gold)를 Neo4j AFFECTS 엣지로 적재.

입력: Gold `reference_book_evidence.csv`
  기본은 로컬 data/gold/reference_book 최신 run, `--s3-latest`면 S3 INCI_data_gold/reference_book 최신 run.
적재:
  (Ingredient {inci_name})-[:AFFECTS {evidence_type:'reference_book'}]->(Effect {effect_code})
  - evidence_type을 MERGE 키에 넣어 논문(pubmed_evidence)·CosIng(cosing_function) 엣지와 별개로 둔다.
  - 속성: type='improves', graph_score=REFERENCE_BOOK_GRAPH_SCORE(고정), paper_count=0,
          claim_scope, medical_wording, print_page, ref_run_id
  - graph_score는 크기에 의미가 없는 고정값이다(근거 종류 간 순서는 서버의 ev_rank가 정함). CosIng 최대 0.15보다
    높게 두어 같은 순위 안에서 CosIng 기능 표기보다 앞선다.
  - 그래프에 있는 Ingredient만 쓰고 노드를 만들지 않는다. 단 Effect BLEMISH_CARE와 ACNE 연결은 없으면 만든다
    (GraphRAG 분류표와 같은 값, 벌크 재임포트 전 라이브 그래프용).
  - 이번 run에 없는 reference_book 엣지는 지운다(전체 갱신, 멱등).
안전장치: 조인되는 근거가 현재 적재된 reference_book 엣지의 절반 미만이면 적재하지 않고 실패.

GraphRAG 벌크 재임포트(그래프 덮어쓰기) 후에도 이 명령으로 다시 적재한다.
롤백:  MATCH ()-[r:AFFECTS {evidence_type:'reference_book'}]->() DELETE r

실행:
  python -m pipeline.reference_book.load_neo4j [--csv <path> | --s3-latest] [--dry-run]
  환경변수: NEO4J_URI, NEO4J_USER(기본 neo4j), NEO4J_PASSWORD, REFERENCE_BOOK_GRAPH_SCORE(기본 0.2)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from common.metadata import write_json
from common.paths import PROJECT_ROOT
from pipeline.reference_book.gold import EVIDENCE_TYPE
from pipeline.reference_book.run import DEFAULT_BUCKET, S3_PREFIXES, SOURCE

OUTPUT_NAME = "reference_book_evidence.csv"
MIN_JOIN_RATIO = 0.5
_RUN_ID_RE = re.compile(r"run_id=([^/\\]+)")
BLEMISH_EFFECT = {"effect_code": "BLEMISH_CARE", "effect_name_en": "Blemish care", "concern": "ACNE"}

ENSURE_EFFECT_CYPHER = """
MERGE (e:Effect {effect_code: $effect_code})
  ON CREATE SET e.effect_name_en = $effect_name_en
WITH e
MATCH (c:Concern {concern_code: $concern})
MERGE (e)-[:RELATES_TO]->(c)
"""

APPLY_CYPHER = """
UNWIND $rows AS row
MATCH (i:Ingredient {inci_name: row.inci_name})
MATCH (e:Effect {effect_code: row.effect_code})
MERGE (i)-[r:AFFECTS {evidence_type: $evidence_type}]->(e)
SET r.type = 'improves', r.graph_score = $score, r.paper_count = 0,
    r.claim_scope = row.claim_scope, r.medical_wording = row.medical_wording,
    r.print_page = row.print_page, r.ref_run_id = $run_id
WITH count(r) AS merged
OPTIONAL MATCH ()-[old:AFFECTS {evidence_type: $evidence_type}]->()
WHERE old.ref_run_id <> $run_id
DELETE old
RETURN merged, count(old) AS removed
"""


def load_rows(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r["evidence_type"] != EVIDENCE_TYPE:
                raise ValueError(f"evidence_type이 {EVIDENCE_TYPE}가 아님: {r['evidence_type']!r}")
            rows.append({
                "inci_name": r["inci_name"].strip(), "effect_code": r["effect_code"].strip(),
                "claim_scope": r["claim_scope"], "print_page": r["print_page"],
                "medical_wording": str(r.get("medical_wording", "")).strip().lower() == "true",
            })
    if not rows:
        raise ValueError(f"빈 CSV: {csv_path}")
    keys = [(r["inci_name"], r["effect_code"]) for r in rows]
    if len(set(keys)) != len(keys):
        raise ValueError("(inci_name, effect_code) 중복")
    return rows


def latest_local(data_root: Path) -> Path:
    found = sorted((data_root / "gold" / SOURCE).glob(f"run_id=*/{OUTPUT_NAME}"), reverse=True)
    if not found:
        raise FileNotFoundError(f"{OUTPUT_NAME}가 없습니다: {data_root / 'gold' / SOURCE}")
    return found[0]


def download_s3_latest(dest: Path) -> Path:
    from pipeline.silver_mapping.kcia_cosing.s3_io import get_s3_client

    bucket = os.getenv("S3_BUCKET", DEFAULT_BUCKET)
    s3 = get_s3_client()
    keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=S3_PREFIXES["gold"] + "/") for o in page.get("Contents", [])
        if o["Key"].endswith("/" + OUTPUT_NAME)]
    if not keys:
        raise FileNotFoundError(f"s3://{bucket}/{S3_PREFIXES['gold']}/ 에 {OUTPUT_NAME}가 없습니다")
    key = sorted(keys)[-1]
    target = dest / key
    target.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(target))
    return target


def run_id_of(csv_path: Path) -> str:
    m = _RUN_ID_RE.search(str(csv_path.resolve()))
    return m.group(1) if m else csv_path.stem


def check_guards(joinable: int, currently_loaded: int) -> list[str]:
    if currently_loaded and joinable < currently_loaded * MIN_JOIN_RATIO:
        return [f"조인 근거 {joinable}건 < 현재 적재 {currently_loaded}건의 {MIN_JOIN_RATIO:.0%} (입력 이상 의심)"]
    return []


def main() -> None:
    from neo4j import GraphDatabase

    ap = argparse.ArgumentParser(description="참고 도서 근거 → Neo4j AFFECTS 적재")
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--csv", type=Path)
    src.add_argument("--s3-latest", action="store_true", help="S3 Gold 최신 run을 받아 적재")
    ap.add_argument("--data-root", type=Path, default=PROJECT_ROOT / "data")
    ap.add_argument("--dry-run", action="store_true", help="적재 없이 조인 결과·안전장치만 확인")
    args = ap.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    uri, password = os.getenv("NEO4J_URI", "").strip(), os.getenv("NEO4J_PASSWORD", "")
    if not uri or not password:
        raise SystemExit("NEO4J_URI / NEO4J_PASSWORD가 설정되지 않았습니다 (.env)")
    score = float(os.getenv("REFERENCE_BOOK_GRAPH_SCORE", "0.2"))

    tmp = tempfile.TemporaryDirectory() if args.s3_latest else None
    csv_path = (download_s3_latest(Path(tmp.name)) if args.s3_latest
                else args.csv or latest_local(args.data_root))
    rows = load_rows(csv_path)
    run_id = run_id_of(csv_path)

    driver = GraphDatabase.driver(uri, auth=(os.getenv("NEO4J_USER", "neo4j"), password))
    try:
        with driver.session() as s:
            if not args.dry_run:
                s.execute_write(lambda tx: tx.run(ENSURE_EFFECT_CYPHER, **BLEMISH_EFFECT).consume())
            ingredients = set(s.run(
                "UNWIND $x AS n MATCH (i:Ingredient {inci_name:n}) RETURN collect(n) AS ok",
                x=sorted({r["inci_name"] for r in rows})).single()["ok"])
            effects = set(s.run("MATCH (e:Effect) RETURN collect(e.effect_code) AS ok").single()["ok"])
            if args.dry_run:
                effects.add(BLEMISH_EFFECT["effect_code"])
            joinable = [r for r in rows if r["inci_name"] in ingredients and r["effect_code"] in effects]
            loaded = s.run("MATCH ()-[r:AFFECTS {evidence_type:$t}]->() RETURN count(r) AS n",
                           t=EVIDENCE_TYPE).single()["n"]
            problems = check_guards(len(joinable), loaded)
            result = {
                "run_id": run_id, "csv": str(csv_path), "dry_run": args.dry_run, "graph_score": score,
                "csv_rows": len(rows), "joinable": len(joinable), "previously_loaded": loaded,
                "joinable_by_effect": {e: sum(r["effect_code"] == e for r in joinable)
                                       for e in sorted({r["effect_code"] for r in joinable})},
                "missing_effects": sorted({r["effect_code"] for r in rows} - effects),
                "guard_problems": problems, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            if problems:
                result["status"] = "blocked"
            elif args.dry_run:
                result["status"] = "dry_run"
            else:
                rec = s.execute_write(lambda tx: tx.run(
                    APPLY_CYPHER, rows=joinable, evidence_type=EVIDENCE_TYPE, score=score,
                    run_id=run_id).single())
                result.update(status="loaded", merged=rec["merged"], removed_stale=rec["removed"])
    finally:
        driver.close()
        if tmp:
            tmp.cleanup()

    if not args.s3_latest:
        write_json(csv_path.parent / "load_result.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if problems:
        raise SystemExit("안전장치로 적재 중단: " + " / ".join(problems))


if __name__ == "__main__":
    main()
