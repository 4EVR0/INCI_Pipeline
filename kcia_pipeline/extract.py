import json
import time
from dataclasses import asdict
from pathlib import Path
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


def _write_checkpoint_state(
    checkpoint_state_path: Path,
    *,
    total_expected: int,
    per_page: int,
    total_pages: int,
    last_completed_page: int,
    total_collected: int,
    status: str,
) -> None:
    payload = {
        "total_expected": total_expected,
        "per_page": per_page,
        "total_pages": total_pages,
        "last_completed_page": last_completed_page,
        "total_collected": total_collected,
        "status": status,
    }
    checkpoint_state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_state_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _append_rows_jsonl(checkpoint_rows_path: Path, rows: List[KciaRawRow]) -> None:
    checkpoint_rows_path.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_rows_path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")


def _load_rows_jsonl(checkpoint_rows_path: Path) -> List[KciaRawRow]:
    rows: List[KciaRawRow] = []
    with open(checkpoint_rows_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(KciaRawRow(**json.loads(line)))
    return rows


def _start_fresh_checkpoint(
    checkpoint_state_path: Path,
    checkpoint_rows_path: Path,
) -> None:
    checkpoint_state_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_rows_path.parent.mkdir(parents=True, exist_ok=True)

    if checkpoint_state_path.exists():
        checkpoint_state_path.unlink()
    if checkpoint_rows_path.exists():
        checkpoint_rows_path.unlink()


def extract_all(settings, checkpoint_state_path: Path, checkpoint_rows_path: Path) -> Tuple[List[KciaRawRow], CrawlStats]:
    logger.info("Starting KCIA extraction")

    session = requests.Session()

    if settings.resume_enabled and checkpoint_state_path.exists() and checkpoint_rows_path.exists():
        logger.info("Checkpoint detected. Loading resume state.")
        with open(checkpoint_state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

        all_rows = _load_rows_jsonl(checkpoint_rows_path)
        total_expected = int(state["total_expected"])
        per_page = int(state["per_page"])
        total_pages = int(state["total_pages"])
        last_completed_page = int(state["last_completed_page"])

        logger.info(
            "Resuming from checkpoint | last_completed_page=%s | total_pages=%s | loaded_rows=%s",
            last_completed_page,
            total_pages,
            len(all_rows),
        )

        if last_completed_page >= total_pages:
            stats = CrawlStats(
                total_expected=total_expected,
                total_collected=len(all_rows),
            )
            logger.info("Checkpoint already complete. Returning resumed rows.")
            return all_rows, stats

        start_page = last_completed_page + 1

    else:
        logger.info("No valid checkpoint found. Starting fresh extraction.")
        _start_fresh_checkpoint(checkpoint_state_path, checkpoint_rows_path)

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

        rows_page_1 = parse_page_rows(first_html)
        logger.info("Parsed page 1 | rows=%s", len(rows_page_1))

        all_rows = list(rows_page_1)

        _append_rows_jsonl(checkpoint_rows_path, rows_page_1)
        _write_checkpoint_state(
            checkpoint_state_path,
            total_expected=total_expected,
            per_page=per_page,
            total_pages=total_pages,
            last_completed_page=1,
            total_collected=len(all_rows),
            status="in_progress",
        )

        if total_pages <= 1:
            _write_checkpoint_state(
                checkpoint_state_path,
                total_expected=total_expected,
                per_page=per_page,
                total_pages=total_pages,
                last_completed_page=1,
                total_collected=len(all_rows),
                status="complete",
            )
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

        start_page = 2
        time.sleep(settings.request_sleep)

    for page in range(start_page, total_pages + 1):
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
        _append_rows_jsonl(checkpoint_rows_path, page_rows)
        _write_checkpoint_state(
            checkpoint_state_path,
            total_expected=total_expected,
            per_page=per_page,
            total_pages=total_pages,
            last_completed_page=page,
            total_collected=len(all_rows),
            status="in_progress",
        )

        time.sleep(settings.request_sleep)

    _write_checkpoint_state(
        checkpoint_state_path,
        total_expected=total_expected,
        per_page=per_page,
        total_pages=total_pages,
        last_completed_page=total_pages,
        total_collected=len(all_rows),
        status="complete",
    )

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