#크롤러 실행 코드
import logging

from config import get_settings
from crawler import crawl_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def main():
    settings = get_settings()
    crawl_all(settings)

if __name__ == "__main__":
    main()