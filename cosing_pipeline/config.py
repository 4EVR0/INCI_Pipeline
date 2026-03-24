import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    api_url: str
    api_key: str
    page_size: int
    timeout: int
    max_retries: int
    sleep_sec: float
    safe_limit: int
    output_dir: Path
    log_dir: Path
    seed_chars: list[str]
    next_chars: list[str]
    user_agent: str
    ingest_date: str
    batch_id: str
    s3_bucket: str
    s3_prefix: str
    strict_validation: bool


def get_settings() -> Settings:
    ingest_date = os.getenv("INGEST_DATE") or datetime.now().strftime("%Y-%m-%d")
    base_dir = Path(__file__).resolve().parent

    api_url = os.getenv("COSING_API_URL", "https://api.tech.ec.europa.eu/search-api/prod/rest/search")
    s3_bucket = os.getenv("S3_BUCKET")
    if not s3_bucket:
        raise ValueError("S3_BUCKET is not set")

    output_dir = base_dir.parent / os.getenv("COSING_OUTPUT_DIR", "output")
    log_dir = base_dir.parent / os.getenv("COSING_LOG_DIR", "logs")
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        api_url=api_url,
        api_key=os.getenv("COSING_API_KEY", ""),
        page_size=int(os.getenv("COSING_PAGE_SIZE", "100")),
        timeout=int(os.getenv("COSING_TIMEOUT", "30")),
        max_retries=int(os.getenv("COSING_MAX_RETRIES", "5")),
        sleep_sec=float(os.getenv("COSING_SLEEP_SEC", "0.2")),
        safe_limit=int(os.getenv("COSING_SAFE_LIMIT", "9500")),
        output_dir=output_dir,
        log_dir=log_dir,
        seed_chars=list(os.getenv("COSING_SEED_CHARS", "abcdefghijklmnopqrstuvwxyz0123456789")),
        next_chars=list(os.getenv("COSING_NEXT_CHARS", "abcdefghijklmnopqrstuvwxyz0123456789")),
        user_agent=os.getenv(
            "COSING_USER_AGENT",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36",
        ),
        ingest_date=ingest_date,
        batch_id=f"cosing_{ingest_date}",
        s3_bucket=s3_bucket,
        s3_prefix=os.getenv("COSING_S3_PREFIX", "INCI_data/cosing"),
        strict_validation=os.getenv("STRICT_VALIDATION", "true").lower() == "true",
    )
