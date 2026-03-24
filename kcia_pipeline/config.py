import os
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
    batch_id: str
    strict_count_check: bool
    user_agent: str


def get_settings() -> Settings:
    ingest_date = os.getenv("INGEST_DATE") or datetime.now().strftime("%Y-%m-%d")

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
        request_sleep=float(os.getenv("KCIA_REQUEST_SLEEP", "0.3")),
        timeout=int(os.getenv("KCIA_TIMEOUT", "15")),
        max_retries=int(os.getenv("KCIA_MAX_RETRIES", "5")),
        ingest_date=ingest_date,
        batch_id=f"kcia_{ingest_date}",
        strict_count_check=os.getenv("KCIA_STRICT_COUNT_CHECK", "true").lower() == "true",
        user_agent=(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"),
    )