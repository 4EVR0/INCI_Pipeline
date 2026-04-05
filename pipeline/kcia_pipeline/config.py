import os
import re
from dataclasses import dataclass
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    kcia_base_url: str
    s3_bucket: str
    s3_prefix: str

    request_sleep: float
    timeout: int
    max_retries: int

    ingest_date: str
    batch_month: str
    batch_id: str

    strict_count_check: bool
    user_agent: str

    resume_enabled: bool
    clear_checkpoint_on_success: bool


def _validate_batch_month(batch_month: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", batch_month):
        raise ValueError("BATCH_MONTH must be in YYYY-MM format. e.g. 2026-04")
    return batch_month


def _validate_ingest_date(ingest_date: str) -> str:
    try:
        datetime.strptime(ingest_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("INGEST_DATE must be in YYYY-MM-DD format. e.g. 2026-04-01") from e
    return ingest_date


def _derive_batch_month(ingest_date: str) -> str:
    dt = datetime.strptime(ingest_date, "%Y-%m-%d")
    return dt.strftime("%Y-%m")


def get_settings() -> Settings:
    ingest_date = os.getenv("INGEST_DATE") or datetime.now().strftime("%Y-%m-%d")
    ingest_date = _validate_ingest_date(ingest_date)

    override_batch_month = os.getenv("BATCH_MONTH")
    if override_batch_month:
        batch_month = _validate_batch_month(override_batch_month)
    else:
        batch_month = _derive_batch_month(ingest_date)

    kcia_base_url = os.getenv("KCIA_BASE_URL")
    s3_bucket = os.getenv("S3_BUCKET")

    if not kcia_base_url:
        raise ValueError("KCIA_BASE_URL is not set")
    if not s3_bucket:
        raise ValueError("S3_BUCKET is not set")

    return Settings(
        kcia_base_url=kcia_base_url,
        s3_bucket=s3_bucket,
        s3_prefix=os.getenv("KCIA_S3_PREFIX", "INCI_data/kcia"),
        request_sleep=float(os.getenv("KCIA_REQUEST_SLEEP", "0.35")),
        timeout=int(os.getenv("KCIA_TIMEOUT", "15")),
        max_retries=int(os.getenv("KCIA_MAX_RETRIES", "5")),
        ingest_date=ingest_date,
        batch_month=batch_month,
        batch_id=f"kcia_{batch_month}",
        strict_count_check=os.getenv("KCIA_STRICT_COUNT_CHECK", "true").lower() == "true",
        user_agent=os.getenv(
            "KCIA_USER_AGENT",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36",
        ),
        resume_enabled=os.getenv("KCIA_RESUME_ENABLED", "true").lower() == "true",
        clear_checkpoint_on_success=os.getenv(
            "KCIA_CLEAR_CHECKPOINT_ON_SUCCESS", "true"
        ).lower() == "true",
    )