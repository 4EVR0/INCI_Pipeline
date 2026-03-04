import random
import time
import logging
from typing import Dict

import requests

logger = logging.getLogger("kcia_crawler")

def build_headers(user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

def fetch_html(
    session: requests.Session,
    base_url: str,
    params: Dict[str, str],
    headers: Dict[str, str],
    timeout: int,
    max_retries: int,
) -> str:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(base_url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()

            # 인코딩 안전 처리
            if r.encoding is None or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding

            return r.text
        except Exception as e:
            last_exc = e
            sleep_s = min(2 ** attempt, 20) + random.uniform(0, 0.7)
            logger.warning(f"Request failed (attempt {attempt}/{max_retries}): {e} | sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed after {max_retries} retries: {last_exc}")