from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

API_KEY = os.getenv("COSING_API_KEY", "")
PAGE_SIZE = int(os.getenv("COSING_PAGE_SIZE", "100"))
TIMEOUT = int(os.getenv("COSING_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("COSING_MAX_RETRIES", "5"))
SLEEP_SEC = float(os.getenv("COSING_SLEEP_SEC", "0.2"))

SAFE_LIMIT = int(os.getenv("COSING_SAFE_LIMIT", "9500"))

OUTPUT_DIR = BASE_DIR / os.getenv("COSING_OUTPUT_DIR", "data")
LOG_DIR = BASE_DIR / os.getenv("COSING_LOG_DIR", "logs")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}

DEFAULT_SEED_CHARS = list(
    os.getenv("COSING_SEED_CHARS", "abcdefghijklmnopqrstuvwxyz0123456789")
)
DEFAULT_NEXT_CHARS = list(
    os.getenv("COSING_NEXT_CHARS", "abcdefghijklmnopqrstuvwxyz0123456789")
)