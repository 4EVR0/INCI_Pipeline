import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import API_URL, API_KEY, HEADERS, TIMEOUT, MAX_RETRIES, SLEEP_SEC
from src.utils import setup_logger


class CosIngClient:
    def __init__(self, log_dir):
        self.logger = setup_logger(log_dir)
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=MAX_RETRIES,
            read=MAX_RETRIES,
            connect=MAX_RETRIES,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["POST"]),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)
        return session

    def search(
        self,
        page_number: int,
        page_size: int = 100,
        text: str = "*",
        extra_form_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params = {
            "apiKey": API_KEY,
            "text": text,
            "pageSize": page_size,
            "pageNumber": page_number,
        }

        data = extra_form_fields or {}

        self.logger.info(
            f"Requesting page={page_number}, page_size={page_size}, text={text}"
        )

        resp = self.session.post(
            API_URL,
            params=params,
            data=data,
            timeout=TIMEOUT,
        )

        self.logger.info(
            f"Response page={page_number}, status={resp.status_code}, bytes={len(resp.content)}"
        )

        resp.raise_for_status()

        try:
            payload = resp.json()
        except Exception as e:
            self.logger.exception("JSON decode failed")
            raise RuntimeError("Failed to decode JSON response") from e

        time.sleep(SLEEP_SEC)
        return payload