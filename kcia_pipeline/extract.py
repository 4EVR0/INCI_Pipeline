import time
from typing import Dict, List, Tuple

import requests

from kcia_pipeline.http_client import fetch_html
from kcia_pipeline.models import CrawlStats, KciaRawRow
from kcia_pipeline.parser import compute_total_pages, parse_page_rows
from kcia_pipeline.utils.logging_utils import setup_logger

logger = setup_logger(__name__)


def make_params(page: int) -> Dict[str, str]:
    return {
        "skind": "ALL",
        "sword": "",
        "sword2": "",
        "page": str(page),
    }


def extract_all(settings) -> Tuple[List[KciaRawRow], CrawlStats]:
    logger.info("Starting KCIA extraction")

    session = requests.Session()

    logger.info("Fetching first page")
    first_html = fetch_html(
        session=session,
        base_url=settings.kcia_base_url,
        params=make_params(1),
        timeout=settings.timeout,
        max_retries=settings.max_retries,
        user_agent=settings.user_agent,
    )

    total_expected, per_page, total_pages = compute_total_pages(first_html)
    logger.info(
        "Parsed pagination info | total_expected=%s | per_page=%s | total_pages=%s",
        total_expected,
        per_page,
        total_pages,
    )

    all_rows: List[KciaRawRow] = []

    rows_page_1 = parse_page_rows(first_html)
    logger.info("Parsed page 1 | rows=%s", len(rows_page_1))
    all_rows.extend(rows_page_1)

    if total_pages <= 1:
        stats = CrawlStats(
            total_expected=total_expected,
            total_collected=len(all_rows),
        )
        logger.info(
            "Extraction completed | total_collected=%s | total_expected=%s",
            stats.total_collected,
            stats.total_expected,
        )
        return all_rows, stats

    time.sleep(settings.request_sleep)

    for page in range(2, total_pages + 1):
        logger.info("Fetching page %s/%s", page, total_pages)

        html = fetch_html(
            session=session,
            base_url=settings.kcia_base_url,
            params=make_params(page),
            timeout=settings.timeout,
            max_retries=settings.max_retries,
            user_agent=settings.user_agent,
        )

        page_rows = parse_page_rows(html)
        logger.info(
            "Parsed page %s | rows=%s | collected_so_far=%s",
            page,
            len(page_rows),
            len(all_rows) + len(page_rows),
        )
        all_rows.extend(page_rows)

        time.sleep(settings.request_sleep)

    stats = CrawlStats(
        total_expected=total_expected,
        total_collected=len(all_rows),
    )

    logger.info(
        "Extraction completed | total_collected=%s | total_expected=%s",
        stats.total_collected,
        stats.total_expected,
    )

    return all_rows, stats