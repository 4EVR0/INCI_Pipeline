from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from oliveyoung_common.batch import create_batch_metadata
from oliveyoung_common.s3_paths import INCI_GOLD_PREFIX

load_dotenv()

_RUN_ID_RE = re.compile(r"^run_id=([a-z_]+_\d{8}_\d{6})$")
MATCHED_FINAL_NAME = "kcia_cosing_matched_final.csv"
GRAPHRAG_MAP_NAME  = "kcia_cosing_graphrag_map.csv"


@dataclass(frozen=True)
class GoldSettings:
    base_dir: Path
    silver_matched_path: Path
    silver_graphrag_path: Path
    gold_output_dir: Path
    batch_month: str
    batch_job: str
    batch_date: datetime
    run_id: str
    s3_bucket: str
    s3_gold_prefix: str


def _discover_latest_silver_file(base_dir: Path, filename: str) -> tuple[str, Path]:
    silver_root = base_dir / "data" / "silver" / "kcia_cosing"
    if not silver_root.is_dir():
        raise FileNotFoundError(f"실버 루트가 없습니다: {silver_root}")

    candidates: list[tuple[str, Path]] = []
    for child in silver_root.iterdir():
        if not child.is_dir():
            continue
        m = _RUN_ID_RE.match(child.name)
        if not m:
            continue
        csv_path = child / filename
        if csv_path.is_file():
            candidates.append((m.group(1), csv_path))

    if not candidates:
        raise FileNotFoundError(
            f"{filename}이 있는 batch_job= 폴더를 찾지 못했습니다: {silver_root}"
        )

    candidates.sort(reverse=True)
    return candidates[0]


def get_gold_settings() -> GoldSettings:
    base_dir = Path(__file__).resolve().parent.parent.parent.parent

    _, silver_matched  = _discover_latest_silver_file(base_dir, MATCHED_FINAL_NAME)
    _, silver_graphrag = _discover_latest_silver_file(base_dir, GRAPHRAG_MAP_NAME)
    silver_path = silver_matched

    gold_output_dir = base_dir / "data" / "gold"
    gold_output_dir.mkdir(parents=True, exist_ok=True)

    batch = create_batch_metadata("inci_gold")
    batch_month = batch.batch_date.strftime("%Y-%m")

    return GoldSettings(
        base_dir=base_dir,
        silver_matched_path=silver_path,
        silver_graphrag_path=silver_graphrag,
        gold_output_dir=gold_output_dir,
        batch_month=batch_month,
        batch_job=batch.batch_job,
        batch_date=batch.batch_date,
        run_id=batch.run_id,
        s3_bucket=os.getenv("S3_BUCKET", "").strip(),
        s3_gold_prefix=os.getenv("S3_GOLD_PREFIX", INCI_GOLD_PREFIX),
    )
