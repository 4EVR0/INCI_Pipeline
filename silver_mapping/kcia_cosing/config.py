from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    data_dir: Path
    output_dir: Path
    review_dir: Path

    # input mode
    input_mode: str  # local | s3
    kcia_local_path: Optional[Path]
    cosing_local_path: Optional[Path]

    # s3
    s3_bucket: str
    kcia_s3_prefix: str
    cosing_s3_prefix: str
    aws_region: str

    # run options
    kcia_ingest_date: Optional[str]
    cosing_ingest_date: Optional[str]
    fuzzy_auto_threshold: int
    fuzzy_review_threshold: int
    save_intermediate: bool


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_settings() -> Settings:
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"
    output_dir = data_dir / "silver"
    review_dir = data_dir / "review"
    output_dir.mkdir(parents=True, exist_ok=True)
    review_dir.mkdir(parents=True, exist_ok=True)

    kcia_local_default = data_dir / "input" / "kcia_2026-03-24.csv"
    cosing_local_default = data_dir / "input" / "cosing_bronze_2026-03-24.csv"

    return Settings(
        base_dir=base_dir,
        data_dir=data_dir,
        output_dir=output_dir,
        review_dir=review_dir,
        input_mode=os.getenv("MAPPING_INPUT_MODE", "local").strip().lower(),
        kcia_local_path=Path(os.getenv("KCIA_LOCAL_PATH", str(kcia_local_default))),
        cosing_local_path=Path(os.getenv("COSING_LOCAL_PATH", str(cosing_local_default))),
        s3_bucket=os.getenv("S3_BUCKET", "oliveyoung-crawl-data"),
        kcia_s3_prefix=os.getenv("KCIA_S3_PREFIX", "INCI_data/kcia"),
        cosing_s3_prefix=os.getenv("COSING_S3_PREFIX", "INCI_data/cosing"),
        aws_region=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
        kcia_ingest_date=os.getenv("KCIA_INGEST_DATE") or None,
        cosing_ingest_date=os.getenv("COSING_INGEST_DATE") or None,
        fuzzy_auto_threshold=int(os.getenv("FUZZY_AUTO_THRESHOLD", "95")),
        fuzzy_review_threshold=int(os.getenv("FUZZY_REVIEW_THRESHOLD", "90")),
        save_intermediate=_to_bool(os.getenv("SAVE_INTERMEDIATE", "true"), default=True),
    )
