import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    kcia_base_url: str = os.getenv("KCIA_BASE_URL", "https://kcia.or.kr/cid/search/ingd_list.php")
    database_url: str = os.getenv("DATABASE_URL", "")
    request_sleep: float = float(os.getenv("REQUEST_SLEEP", "0.35"))
    timeout: int = int(os.getenv("TIMEOUT", "15"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))

    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )

def get_settings() -> Settings:
    s = Settings()
    if not s.database_url:
        raise RuntimeError("DATABASE_URL is required in .env")
    return s