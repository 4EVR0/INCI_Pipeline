import time
from typing import List, Tuple
from http_client import fetch_html
from parser import parse_total_count, parse_rows
from models import KciaRawRow, CrawlStats


def extract_all(settings) -> Tuple[List[KciaRawRow], CrawlStats]:
    rows = []

    page = 1
    total_expected = None

    while True:
        params = {"page": page}
        html = fetch_html(settings.kcia_base_url, params, settings)

        if page == 1:
            total_expected = parse_total_count(html)

        page_rows = parse_rows(html)

        if not page_rows:
            break

        rows.extend(page_rows)
        page += 1
        time.sleep(settings.request_sleep)

    stats = CrawlStats(
        total_expected=total_expected or 0,
        total_collected=len(rows),
    )

    return rows, stats