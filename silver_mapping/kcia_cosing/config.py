from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    output_dir: Path
    review_dir: Path

    batch_month: str
    input_mode: str  # bronze_local | s3

    kcia_local_path: Optional[Path]
    cosing_local_path: Optional[Path]

    s3_bucket: str
    kcia_s3_prefix: str
    cosing_s3_prefix: str
    aws_region: str

    fuzzy_auto_threshold: int
    fuzzy_review_threshold: int
    save_intermediate: bool


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _validate_batch_month(batch_month: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", batch_month):
        raise ValueError("BATCH_MONTH must be in YYYY-MM format. e.g. 2026-03")
    return batch_month


def _derive_batch_month() -> str:
    return datetime.now().strftime("%Y-%m")


def get_settings() -> Settings:
    base_dir = Path(__file__).resolve().parent.parent.parent  # INCI_data
    batch_month = _validate_batch_month(os.getenv("BATCH_MONTH") or _derive_batch_month())

    output_dir = base_dir / "silver" / "kcia_cosing"
    review_dir = output_dir / "review"
    output_dir.mkdir(parents=True, exist_ok=True)
    review_dir.mkdir(parents=True, exist_ok=True)

    kcia_local_default = base_dir / "bronze" / "kcia" / f"batch={batch_month}" / "kcia_bronze.csv"
    cosing_local_default = base_dir / "bronze" / "cosing" / f"batch={batch_month}" / "cosing_bronze.csv"

    return Settings(
        base_dir=base_dir,
        output_dir=output_dir,
        review_dir=review_dir,
        batch_month=batch_month,
        input_mode=os.getenv("MAPPING_INPUT_MODE", "bronze_local").strip().lower(),
        kcia_local_path=Path(os.getenv("KCIA_LOCAL_PATH", str(kcia_local_default))),
        cosing_local_path=Path(os.getenv("COSING_LOCAL_PATH", str(cosing_local_default))),
        s3_bucket=os.getenv("S3_BUCKET", "oliveyoung-crawl-data"),
        kcia_s3_prefix=os.getenv("KCIA_S3_PREFIX", "INCI_data/kcia"),
        cosing_s3_prefix=os.getenv("COSING_S3_PREFIX", "INCI_data/cosing"),
        aws_region=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
        fuzzy_auto_threshold=int(os.getenv("FUZZY_AUTO_THRESHOLD", "95")),
        fuzzy_review_threshold=int(os.getenv("FUZZY_REVIEW_THRESHOLD", "90")),
        save_intermediate=_to_bool(os.getenv("SAVE_INTERMEDIATE", "true"), default=True),
    )