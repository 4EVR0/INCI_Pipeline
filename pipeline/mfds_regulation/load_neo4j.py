"""국내 규제 상태를 Neo4j Ingredient에 적재 (월간 DAG 마지막 단계).

입력: data/gold/mfds_regulation/run_id=<id>/ingredient_kr_regulation.csv (기본: 최신 run)
- CSV 성분 중 그래프에 있는 Ingredient(inci_name 일치)에만 kr_reg_status, kr_limit_note,
  kr_reg_run_id를 SET한다. CSV에 없는 Ingredient는 'none'으로 재설정(전체 갱신, 멱등).
- 노드·관계는 만들거나 지우지 않는다.
- 안전장치: 아래면 적재하지 않고 실패(사람 검토 필요).
    · 그래프 조인 성분 수가 현재 규제 표시 성분 수의 절반 미만 (CSV/이름 이상 의심)
    · 조인되는 banned 성분 수가 KR_REG_MAX_BANNED(기본 10) 초과
- 결과는 CSV 옆 load_result.json에 남긴다.

GraphRAG_Pipeline scripts/load_kr_regulation_to_neo4j.py 와 같은 Cypher를 쓴다
(벌크 재임포트 후 수동 재적재용).

롤백:  MATCH (i:Ingredient) REMOVE i.kr_reg_status, i.kr_limit_note, i.kr_reg_run_id

실행:
  python -m pipeline.mfds_regulation.load_neo4j [--csv <path>] [--dry-run]
  환경변수: NEO4J_URI, NEO4J_USER(기본 neo4j), NEO4J_PASSWORD, KR_REG_MAX_BANNED
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from common.metadata import write_json
from common.paths import PROJECT_ROOT

STATUSES = {"banned", "conditional", "restricted"}
OUTPUT_NAME = "ingredient_kr_regulation.csv"
_RUN_ID_RE = re.compile(r"run_id=([^/\\]+)")
MIN_JOIN_RATIO = 0.5

APPLY_CYPHER = """
MATCH (i:Ingredient)
WHERE NOT i.inci_name IN $names
SET i.kr_reg_status = 'none', i.kr_reg_run_id = $run_id
REMOVE i.kr_limit_note
WITH count(i) AS reset
UNWIND $rows AS row
MATCH (i:Ingredient {inci_name: row.inci_name})
SET i.kr_reg_status = row.kr_reg_status,
    i.kr_limit_note = CASE WHEN row.kr_limit_note = '' THEN null ELSE row.kr_limit_note END,
    i.kr_reg_run_id = $run_id
RETURN reset, count(i) AS updated
"""


def latest_output(data_root: Path) -> Path:
    candidates = sorted((data_root / "gold" / "mfds_regulation").glob(f"run_id=*/{OUTPUT_NAME}"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"{OUTPUT_NAME}가 없습니다: {data_root / 'gold' / 'mfds_regulation'}")
    return candidates[0]


def load_rows(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            status = r["kr_reg_status"].strip()
            if status not in STATUSES:
                raise ValueError(f"알 수 없는 kr_reg_status: {status!r} ({r['inci_name']})")
            rows.append({
                "inci_name": r["inci_name"].strip(),
                "kr_reg_status": status,
                "kr_limit_note": (r.get("kr_limit_note") or "").strip() if status == "restricted" else "",
            })
    names = [r["inci_name"] for r in rows]
    if not rows:
        raise ValueError(f"빈 CSV: {csv_path}")
    if len(set(names)) != len(names):
        raise ValueError("inci_name 중복")
    return rows


def run_id_of(csv_path: Path) -> str:
    m = _RUN_ID_RE.search(str(csv_path.resolve()))
    return m.group(1) if m else csv_path.stem


def check_guards(joinable: list[dict], currently_tagged: int, max_banned: int) -> list[str]:
    """적재를 막아야 하는 이유 목록. 비어 있으면 적재 가능."""
    problems = []
    if currently_tagged and len(joinable) < currently_tagged * MIN_JOIN_RATIO:
        problems.append(f"조인 성분 {len(joinable)}개 < 현재 규제 표시 {currently_tagged}개의 "
                        f"{MIN_JOIN_RATIO:.0%} (CSV 또는 이름 이상 의심)")
    banned = sorted(r["inci_name"] for r in joinable if r["kr_reg_status"] == "banned")
    if len(banned) > max_banned:
        problems.append(f"banned {len(banned)}개 > 한도 {max_banned}개: {', '.join(banned[:20])}")
    return problems


def main() -> None:
    from neo4j import GraphDatabase

    ap = argparse.ArgumentParser(description="국내 규제 상태 → Neo4j Ingredient 적재")
    ap.add_argument("--csv", type=Path, help=f"기본: data/gold/mfds_regulation 최신 {OUTPUT_NAME}")
    ap.add_argument("--data-root", type=Path, default=PROJECT_ROOT / "data")
    ap.add_argument("--dry-run", action="store_true", help="적재 없이 조인 결과·안전장치만 확인")
    args = ap.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    uri = os.getenv("NEO4J_URI", "").strip()
    password = os.getenv("NEO4J_PASSWORD", "")
    if not uri or not password:
        raise SystemExit("NEO4J_URI / NEO4J_PASSWORD가 설정되지 않았습니다 (.env)")
    max_banned = int(os.getenv("KR_REG_MAX_BANNED", "10"))

    csv_path = args.csv or latest_output(args.data_root)
    rows = load_rows(csv_path)
    run_id = run_id_of(csv_path)

    driver = GraphDatabase.driver(uri, auth=(os.getenv("NEO4J_USER", "neo4j"), password))
    try:
        with driver.session() as s:
            names = [r["inci_name"] for r in rows]
            ok = set(s.run("UNWIND $x AS n MATCH (i:Ingredient {inci_name:n}) RETURN collect(n) AS ok",
                           x=names).single()["ok"])
            joinable = [r for r in rows if r["inci_name"] in ok]
            tagged = s.run("MATCH (i:Ingredient) WHERE i.kr_reg_status IN $st RETURN count(i) AS n",
                           st=sorted(STATUSES)).single()["n"]
            result = {
                "run_id": run_id, "csv": str(csv_path), "dry_run": args.dry_run,
                "csv_rows": len(rows), "joinable": len(joinable), "previously_tagged": tagged,
                "joinable_by_status": {st: sum(r["kr_reg_status"] == st for r in joinable)
                                       for st in sorted(STATUSES)},
                "banned": sorted(r["inci_name"] for r in joinable if r["kr_reg_status"] == "banned"),
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            problems = check_guards(joinable, tagged, max_banned)
            result["guard_problems"] = problems
            if problems:
                result["status"] = "blocked"
            elif args.dry_run:
                result["status"] = "dry_run"
            else:
                rec = s.execute_write(lambda tx: tx.run(
                    APPLY_CYPHER, names=[r["inci_name"] for r in joinable], rows=joinable,
                    run_id=run_id).single())
                result.update(status="loaded", reset_to_none=rec["reset"], updated=rec["updated"])
    finally:
        driver.close()

    write_json(csv_path.parent / "load_result.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if problems:
        raise SystemExit("안전장치로 적재 중단: " + " / ".join(problems))


if __name__ == "__main__":
    main()
