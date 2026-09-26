"""Read-only MFDS ingredient API pilot and conservative KCIA/CosIng overlap audit.

No Gold, Iceberg, S3, Neo4j, or recommendation data is changed by this module.
The MFDS ingredient master identifies names and origins; it is not efficacy evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://apis.data.go.kr/1471000/CsmtcsIngdCpntInfoService01/getCsmtcsIngdCpntInfoService01"
SOURCE_URL = "https://www.data.go.kr/data/15111774/openapi.do"
FIELDS = {
    "INGR_KOR_NAME": "mfds_kor_name",
    "INGR_ENG_NAME": "mfds_eng_name",
    "CAS_NO": "mfds_cas_no",
    "ORIGIN_MAJOR_KOR_NAME": "mfds_origin_definition",
    "INGR_SYNONYM": "mfds_synonym",
}
CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")


def _session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=frozenset({"GET"}), raise_on_status=False)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _clean(value: object) -> str:
    return str(value).strip() if value is not None else ""


def parse_page(payload: dict, requested_page: int) -> tuple[list[dict], int]:
    """Accept the common list/single-item JSON shapes, but fail on API errors."""
    if not isinstance(payload, dict):
        raise ValueError("MFDS response must be an object")
    root = payload.get("response", payload)
    if not isinstance(root, dict):
        raise ValueError("MFDS response must be an object")
    header = root.get("header") or {}
    code = str(header.get("resultCode", ""))
    if code not in {"00", "0", "0000"}:
        raise ValueError(f"MFDS resultCode={code or 'missing'}")
    body = root.get("body") or {}
    if not isinstance(body, dict):
        raise ValueError("MFDS body must be an object")
    try:
        total = int(body["totalCount"])
        page = int(body["pageNo"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("MFDS pagination fields are missing or invalid") from exc
    if total < 0 or page != requested_page:
        raise ValueError("MFDS pagination does not match request")

    items = body.get("items") or {}
    rows = items.get("item", []) if isinstance(items, dict) else items
    if rows in (None, ""):
        rows = []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("MFDS items must be objects")
    return rows, total


def fetch_page(session: requests.Session, service_key: str, page: int, page_size: int) -> tuple[list[dict], int]:
    if not service_key:
        raise ValueError("MFDS_API_KEY is required")
    try:
        response = session.get(
            API_URL,
            params={"serviceKey": service_key, "pageNo": page,
                    "numOfRows": page_size, "type": "json"},
            timeout=20,
        )
    except requests.RequestException:
        # requests exceptions may contain a URL with the service key.
        raise RuntimeError("MFDS request failed") from None
    if response.status_code != 200:
        raise RuntimeError(f"MFDS HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError:
        raise ValueError("MFDS returned non-JSON data") from None
    return parse_page(payload, page)


def fetch_snapshot(service_key: str, *, page_size: int = 100, max_pages: int = 1,
                   session: requests.Session | None = None) -> tuple[list[dict], dict]:
    if not 1 <= page_size <= 1000 or max_pages < 1:
        raise ValueError("page_size must be 1..1000 and max_pages must be positive")
    rows: list[dict] = []
    total: int | None = None
    own_session = session is None
    session = session or _session()
    try:
        for page in range(1, max_pages + 1):
            batch, reported_total = fetch_page(session, service_key, page, page_size)
            if total is None:
                total = reported_total
            elif reported_total != total:
                raise ValueError("MFDS totalCount changed during pagination")
            if not batch and len(rows) < total:
                raise ValueError("MFDS returned an empty page before totalCount was reached")
            rows.extend({target: _clean(item.get(source)) for source, target in FIELDS.items()}
                        for item in batch)
            if len(rows) >= total:
                break
            time.sleep(0.2)
    finally:
        if own_session:
            session.close()
    if total is None or len(rows) > total:
        raise ValueError("MFDS item count is inconsistent")
    return rows, {
        "source_url": SOURCE_URL,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "reported_total": total,
        "fetched_rows": len(rows),
        "complete": len(rows) == total,
        "page_size": page_size,
        "max_pages": max_pages,
    }


def _name(value: object) -> str:
    return re.sub(r"\s+", " ", _clean(value)).casefold()


def _cas(value: object) -> set[str]:
    return set(CAS_RE.findall(_clean(value)))


def audit_matches(mfds_rows: list[dict], gold: pd.DataFrame) -> dict:
    """Measure exact-name coverage; never auto-promote MFDS rows into Gold."""
    required = {"inci_name", "kor_name", "eng_name", "kcia_cas_no"}
    missing = required - set(gold.columns)
    if missing:
        raise ValueError(f"Gold columns missing: {', '.join(sorted(missing))}")
    by_name: dict[str, list[dict]] = {}
    for row in mfds_rows:
        for key in {_name(row.get("mfds_eng_name")), _name(row.get("mfds_kor_name"))} - {""}:
            by_name.setdefault(key, []).append(row)

    counts = {status: 0 for status in
              ("name_and_cas", "name_only", "cas_conflict", "ambiguous", "unmatched")}
    examples: dict[str, list[dict]] = {status: [] for status in counts}
    for _, item in gold.iterrows():
        names = {_name(item.get(col)) for col in ("inci_name", "eng_name", "kor_name")} - {""}
        candidates = list({id(row): row for key in names for row in by_name.get(key, [])}.values())
        gold_cas = _cas(item.get("kcia_cas_no"))
        if not candidates:
            status = "unmatched"
        elif len(candidates) > 1:
            status = "ambiguous"
        else:
            source_cas = _cas(candidates[0].get("mfds_cas_no"))
            if gold_cas and source_cas:
                status = "name_and_cas" if gold_cas & source_cas else "cas_conflict"
            else:
                status = "name_only"
        counts[status] += 1
        if len(examples[status]) < 10:
            examples[status].append({
                "inci_name": _clean(item.get("inci_name")),
                "kor_name": _clean(item.get("kor_name")),
                "kcia_cas_no": _clean(item.get("kcia_cas_no")),
                "mfds_candidates": len(candidates),
            })
    return {"gold_rows": len(gold), "mfds_rows": len(mfds_rows),
            "counts": counts, "examples": examples}


def main() -> None:
    parser = argparse.ArgumentParser(description="MFDS 화장품 성분 API 로컬 파일럿 (운영 적재 없음)")
    parser.add_argument("--gold", type=Path, help="기존 Gold CSV; 완전 스냅샷일 때만 커버리지 집계")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    load_dotenv()
    rows, metadata = fetch_snapshot(os.getenv("MFDS_API_KEY", ""),
                                    page_size=args.page_size, max_pages=args.max_pages)
    if args.gold and not metadata["complete"]:
        raise ValueError("Gold coverage requires a complete MFDS snapshot; increase --max-pages")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "mfds_ingredients.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.gold:
        gold = pd.read_csv(args.gold, dtype=str).fillna("")
        metadata["matching_audit"] = audit_matches(rows, gold)
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: metadata[key] for key in ("reported_total", "fetched_rows", "complete")},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
