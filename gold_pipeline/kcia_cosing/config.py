from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_BATCH_DIR_RE = re.compile(r"^batch=(\d{4})-(\d{2})$")
MATCHED_FINAL_NAME = "kcia_cosing_matched_final.csv"


@dataclass(frozen=True)
class GoldSettings:
    base_dir: Path
    silver_matched_path: Path
    gold_output_dir: Path
    batch_month: str
    s3_bucket: str
    s3_gold_prefix: str


def discover_latest_silver_matched_final(base_dir: Path) -> tuple[str, Path]:
    """silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_matched_final.csv 중 가장 최근 배치."""
    silver_root = base_dir / "silver" / "kcia_cosing"
    if not silver_root.is_dir():
        raise FileNotFoundError(f"실버 루트가 없습니다: {silver_root}")

    candidates: list[tuple[tuple[int, int], Path, Path]] = []
    for child in silver_root.iterdir():
        if not child.is_dir():
            continue
        m = _BATCH_DIR_RE.match(child.name)
        if not m:
            continue
        y, mo = int(m.group(1)), int(m.group(2))
        csv_path = child / MATCHED_FINAL_NAME
        if csv_path.is_file():
            candidates.append(((y, mo), child, csv_path))

    if not candidates:
        raise FileNotFoundError(
            f"matched_final CSV가 있는 batch= 폴더를 찾지 못했습니다: {silver_root}"
        )

    candidates.sort(key=lambda x: x[0], reverse=True)
    (y, mo), _dir, csv_path = candidates[0]
    batch_month = f"{y:04d}-{mo:02d}"
    return batch_month, csv_path


def get_gold_settings() -> GoldSettings:
    base_dir = Path(__file__).resolve().parent.parent.parent

    batch_month, silver_path = discover_latest_silver_matched_final(base_dir)

    gold_output_dir = base_dir / "gold"
    gold_output_dir.mkdir(parents=True, exist_ok=True)

    return GoldSettings(
        base_dir=base_dir,
        silver_matched_path=silver_path,
        gold_output_dir=gold_output_dir,
        batch_month=batch_month,
        s3_bucket=os.getenv("S3_BUCKET", "").strip(),
        s3_gold_prefix=os.getenv("S3_GOLD_PREFIX", "INCI_data_gold/").rstrip("/"),
    )
