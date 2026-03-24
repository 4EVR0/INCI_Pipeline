import json
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cosing_pipeline.utils.logging_utils import setup_logger


class CosIngClient:
    def __init__(self, settings, item_type: str = "ingredient", search_field: str = "inciName"):
        self.settings = settings
        self.logger = setup_logger(__name__)
        self.session = self._build_session()
        self.item_type = item_type
        self.search_field = search_field

    def _build_session(self) -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=self.settings.max_retries,
            read=self.settings.max_retries,
            connect=self.settings.max_retries,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["POST"]),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": self.settings.user_agent,
            }
        )
        return session

    def _build_query_blob(
        self,
        text: str,
        search_field: Optional[str] = None,
        item_type: Optional[str] = None,
        extra_must_clauses: Optional[list] = None,
    ) -> Dict[str, Any]:
        search_field = search_field or self.search_field
        item_type = item_type or self.item_type

        must = []

        if text and text != "*":
            must.append(
                {
                    "text": {
                        "query": text,
                        "fields": [search_field],
                        "defaultOperator": "AND",
                    }
                }
            )

        if item_type:
            must.append({"term": {"itemType": item_type}})

        if extra_must_clauses:
            must.extend(extra_must_clauses)

        return {"bool": {"must": must}}

    def search(
        self,
        page_number: int,
        page_size: Optional[int] = None,
        text: str = "*",
        extra_form_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        page_size = page_size or self.settings.page_size
        params = {
            "apiKey": self.settings.api_key,
            "text": text,
            "pageSize": page_size,
            "pageNumber": page_number,
        }

        query_blob = self._build_query_blob(
            text=text,
            search_field=(extra_form_fields or {}).get("search_field"),
            item_type=(extra_form_fields or {}).get("item_type", self.item_type),
            extra_must_clauses=(extra_form_fields or {}).get("extra_must_clauses"),
        )

        files = {
            "query": (
                "blob",
                json.dumps(query_blob, ensure_ascii=False),
                "application/json",
            )
        }

        self.logger.info(
            "Requesting page=%s page_size=%s text=%s item_type=%s",
            page_number,
            page_size,
            text,
            (extra_form_fields or {}).get("item_type", self.item_type),
        )

        response = self.session.post(
            self.settings.api_url,
            params=params,
            files=files,
            timeout=self.settings.timeout,
        )

        self.logger.info(
            "Response page=%s status=%s bytes=%s",
            page_number,
            response.status_code,
            len(response.content),
        )
        response.raise_for_status()

        payload = response.json()
        time.sleep(self.settings.sleep_sec)
        return payload
