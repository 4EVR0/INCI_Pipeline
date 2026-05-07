from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from oliveyoung_common.batch import create_batch_metadata
from oliveyoung_common.s3_paths import BUCKET, INCI_BRONZE_KCIA_PREFIX, INCI_BRONZE_COSING_PREFIX

load_dotenv()


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    output_dir: Path
    review_dir: Path

    batch_month: str
    batch_job: str
    batch_date: datetime
    run_id: str
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


_RUN_ID_RE = re.compile(r"^run_id=([a-z_]+_\d{8}_\d{6})$")


def _discover_latest_bronze_path(base_dir: Path, source: str, filename: str) -> Optional[Path]:
    """data/bronze/{source}/batch_job=YYYYMMDD_HHMMSS/ 중 최신 폴더의 파일 경로를 반환합니다."""
    bronze_root = base_dir / "data" / "bronze" / source
    if not bronze_root.is_dir():
        return None
    candidates = []
    for child in bronze_root.iterdir():
        if not child.is_dir():
            continue
        m = _RUN_ID_RE.match(child.name)
        if not m:
            continue
        csv_path = child / filename
        if csv_path.is_file():
            candidates.append((m.group(1), csv_path))
    if not candidates:
        return None
    candidates.sort(reverse=True)  # 문자열 정렬 → 최신 배치
    return candidates[0][1]


def get_settings() -> Settings:
    base_dir = Path(__file__).resolve().parent.parent.parent.parent  # INCI_data
    batch_month = _validate_batch_month(os.getenv("BATCH_MONTH") or _derive_batch_month())

    output_dir = base_dir / "data" / "silver" / "kcia_cosing"
    review_dir = output_dir / "review"
    output_dir.mkdir(parents=True, exist_ok=True)
    review_dir.mkdir(parents=True, exist_ok=True)

    kcia_local_default = _discover_latest_bronze_path(base_dir, "kcia", "kcia_bronze.csv")
    cosing_local_default = _discover_latest_bronze_path(base_dir, "cosing", "cosing_bronze.csv")

    fuzzy_auto = int(os.getenv("FUZZY_AUTO_THRESHOLD", "95"))
    fuzzy_review = int(os.getenv("FUZZY_REVIEW_THRESHOLD", "90"))
    if fuzzy_review >= fuzzy_auto:
        raise ValueError(
            f"FUZZY_REVIEW_THRESHOLD({fuzzy_review})는 FUZZY_AUTO_THRESHOLD({fuzzy_auto})보다 작아야 합니다."
        )

    batch = create_batch_metadata("silver_mapping")

    return Settings(
        base_dir=base_dir,
        output_dir=output_dir,
        review_dir=review_dir,
        batch_month=batch_month,
        batch_job=batch.batch_job,
        batch_date=batch.batch_date,
        run_id=batch.run_id,
        input_mode=os.getenv("MAPPING_INPUT_MODE", "bronze_local").strip().lower(),
        kcia_local_path=Path(_kcia_env) if (_kcia_env := os.getenv("KCIA_LOCAL_PATH")) else kcia_local_default,
        cosing_local_path=Path(_cosing_env) if (_cosing_env := os.getenv("COSING_LOCAL_PATH")) else cosing_local_default,
        s3_bucket=os.getenv("S3_BUCKET", BUCKET),
        kcia_s3_prefix=os.getenv("KCIA_S3_PREFIX", INCI_BRONZE_KCIA_PREFIX),
        cosing_s3_prefix=os.getenv("COSING_S3_PREFIX", INCI_BRONZE_COSING_PREFIX),
        aws_region=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
        fuzzy_auto_threshold=fuzzy_auto,
        fuzzy_review_threshold=fuzzy_review,
        save_intermediate=_to_bool(os.getenv("SAVE_INTERMEDIATE", "true"), default=True),
    )