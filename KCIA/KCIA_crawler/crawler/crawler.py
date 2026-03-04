import time
import logging
from typing import Dict, Tuple

import requests

from config import Settings
from http_client import fetch_html, build_headers
from parser import compute_total_pages, parse_page_rows
from db import get_conn, init_schema, upsert_rows

logger = logging.getLogger("kcia_crawler")

def make_params(page: int) -> Dict[str, str]:
    # 핵심: sword=+ (공백 검색과 동일하게 전체 데이터 노출되는 케이스)
    return {
        "skind": "ALL",
        "sword": "",
        "sword2": "",
        "page": str(page),
    }

def crawl_all(settings: Settings):
    headers = build_headers(settings.user_agent)
    session = requests.Session()

    # 1) first page
    first_html = fetch_html(
        session=session,
        base_url=settings.kcia_base_url,
        params=make_params(1),
        headers=headers,
        timeout=settings.timeout,
        max_retries=settings.max_retries,
    )
    total, per_page, total_pages = compute_total_pages(first_html)
    logger.info(f"Total={total:,} | per_page={per_page} | total_pages={total_pages:,}")

    # 2) db
    conn = get_conn(settings.database_url)
    try:
        init_schema(conn)

        # 3) page loop
        rows1 = parse_page_rows(first_html)
        upsert_rows(conn, rows1)
        logger.info(f"[page 1/{total_pages}] upserted {len(rows1)} rows")
        time.sleep(settings.request_sleep)

        for page in range(2, total_pages + 1):
            html = fetch_html(
                session=session,
                base_url=settings.kcia_base_url,
                params=make_params(page),
                headers=headers,
                timeout=settings.timeout,
                max_retries=settings.max_retries,
            )
            rows = parse_page_rows(html)
            if not rows:
                logger.warning(f"[page {page}/{total_pages}] No rows parsed.")
                continue

            upsert_rows(conn, rows)
            logger.info(f"[page {page}/{total_pages}] upserted {len(rows)} rows")
            time.sleep(settings.request_sleep)

        logger.info("Done. Verify: SELECT COUNT(*) FROM kcia_ingredient_dict;")
    finally:
        conn.close()