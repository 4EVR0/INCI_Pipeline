from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_kcia_bronze_metadata(
    *,
    batch_month: str,
    ingest_date: str,
    batch_id: str,
    row_count: int,
    total_expected: int,
    total_collected: int,
    local_csv_path: str,
    s3_bucket: str,
    s3_csv_key: str,
) -> Dict[str, Any]:
    return {
        "source": "kcia",
        "layer": "bronze",
        "batch_month": batch_month,
        "ingest_date": ingest_date,
        "batch_id": batch_id,
        "row_count": row_count,
        "crawl_stats": {
            "total_expected": total_expected,
            "total_collected": total_collected,
        },
        "artifacts": {
            "local_csv_path": local_csv_path,
            "s3_bucket": s3_bucket,
            "s3_csv_key": s3_csv_key,
        },
        "created_at_utc": utc_now_iso(),
        "status": "success",
    }