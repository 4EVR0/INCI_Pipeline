from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_BATCH_JOB_RE = re.compile(r"^batch_job=(\d{8}_\d{6})$")
MATCHED_FINAL_NAME = "kcia_cosing_matched_final.csv"


@dataclass(frozen=True)
class GoldSettings:
    base_dir: Path
    silver_matched_path: Path
    gold_output_dir: Path
    batch_month: str
    batch_job: str
    batch_date: datetime
    s3_bucket: str
    s3_gold_prefix: str


def discover_latest_silver_matched_final(base_dir: Path) -> tuple[str, Path]:
    """silver/kcia_cosing/batch_job=YYYYMMDD_HHMMSS/kcia_cosing_matched_final.csv 중 가장 최근 배치."""
    silver_root = base_dir / "data" / "silver" / "kcia_cosing"
    if not silver_root.is_dir():
        raise FileNotFoundError(f"실버 루트가 없습니다: {silver_root}")

    candidates: list[tuple[str, Path]] = []
    for child in silver_root.iterdir():
        if not child.is_dir():
            continue
        m = _BATCH_JOB_RE.match(child.name)
        if not m:
            continue
        csv_path = child / MATCHED_FINAL_NAME
        if csv_path.is_file():
            candidates.append((m.group(1), csv_path))

    if not candidates:
        raise FileNotFoundError(
            f"matched_final CSV가 있는 batch_job= 폴더를 찾지 못했습니다: {silver_root}"
        )

    candidates.sort(reverse=True)  # 문자열 정렬 → 최신 배치
    batch_job, csv_path = candidates[0]
    return batch_job, csv_path


def get_gold_settings() -> GoldSettings:
    base_dir = Path(__file__).resolve().parent.parent.parent.parent

    _, silver_path = discover_latest_silver_matched_final(base_dir)

    gold_output_dir = base_dir / "data" / "gold"
    gold_output_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    batch_job = now_utc.strftime("%Y%m%d_%H%M%S")
    batch_month = now_utc.strftime("%Y-%m")

    return GoldSettings(
        base_dir=base_dir,
        silver_matched_path=silver_path,
        gold_output_dir=gold_output_dir,
        batch_month=batch_month,
        batch_job=batch_job,
        batch_date=now_utc,
        s3_bucket=os.getenv("S3_BUCKET", "").strip(),
        s3_gold_prefix=os.getenv("S3_GOLD_PREFIX", "INCI_data_gold/").rstrip("/"),
    )
