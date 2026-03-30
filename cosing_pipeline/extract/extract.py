import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cosing_pipeline.extract.client import CosIngClient
from cosing_pipeline.extract.splitter import CosIngQuerySplitter
from cosing_pipeline.models import ExtractionStats
from cosing_pipeline.utils.logging_utils import setup_logger


logger = setup_logger(__name__)


def _checkpoint_state_path(output_dir: Path) -> Path:
    return output_dir / "checkpoint_state.json"


def _checkpoint_pages_path(output_dir: Path) -> Path:
    return output_dir / "raw_pages_checkpoint.jsonl"


def _load_checkpoint_state(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_checkpoint_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _append_payload_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_payloads_jsonl(path: Path) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    if not path.exists():
        return payloads

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            payloads.append(json.loads(line))
    return payloads


def _clear_resume_files(output_dir: Path) -> None:
    state_path = _checkpoint_state_path(output_dir)
    pages_path = _checkpoint_pages_path(output_dir)

    if state_path.exists():
        state_path.unlink()
    if pages_path.exists():
        pages_path.unlink()


def extract_all(
    settings,
    *,
    item_type: str = "ingredient",
    max_depth: int = 6,
    max_seeds: Optional[int] = None,
    max_queries: Optional[int] = None,
    resume_count_cache: bool = True,
    extra_form_fields: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], ExtractionStats]:
    client = CosIngClient(settings=settings, item_type=item_type)
    cache_path = settings.output_dir / "count_cache.json" if resume_count_cache else None

    splitter = CosIngQuerySplitter(
        client=client,
        settings=settings,
        cache_path=cache_path,
        extra_form_fields=extra_form_fields,
    )

    queries, oversized_queries = splitter.build_queries(max_depth=max_depth, max_seeds=max_seeds)

    if oversized_queries:
        raise RuntimeError(
            f"Oversized queries remain: {len(oversized_queries)}. "
            "Resolve them before full collection."
        )

    if max_queries is not None:
        queries = queries[:max_queries]

    state_path = _checkpoint_state_path(settings.output_dir)
    pages_path = _checkpoint_pages_path(settings.output_dir)

    raw_pages: List[Dict[str, Any]] = []
    raw_result_count = 0
    completed_query_page_keys = set()

    if settings.resume_enabled:
        state = _load_checkpoint_state(state_path)
        if state and pages_path.exists():
            logger.info("Checkpoint detected. Loading CosIng resume state.")
            raw_pages = _load_payloads_jsonl(pages_path)
            raw_result_count = sum(len(p.get("results", []) or []) for p in raw_pages)

            for p in raw_pages:
                q = p.get("_query")
                pn = int(p.get("pageNumber", 0) or 0)
                completed_query_page_keys.add((q, pn))

            logger.info(
                "Resume loaded | saved_pages=%s | saved_results=%s",
                len(raw_pages),
                raw_result_count,
            )
        else:
            logger.info("No valid CosIng checkpoint found. Starting fresh.")
            _clear_resume_files(settings.output_dir)
    else:
        _clear_resume_files(settings.output_dir)

    for query_idx, query in enumerate(queries, start=1):
        logger.info("Collecting query=%s (%s/%s)", query, query_idx, len(queries))

        first_page_key = (query, 1)

        if first_page_key in completed_query_page_keys:
            logger.info("Skipping already collected first page for query=%s", query)

            existing_first = next(
                (p for p in raw_pages if p.get("_query") == query and int(p.get("pageNumber", 0) or 0) == 1),
                None,
            )
            if existing_first is None:
                raise RuntimeError(f"Checkpoint inconsistency: missing saved first page for query={query}")

            total_results = int(existing_first.get("totalResults", 0) or 0)
            page_size = int(existing_first.get("pageSize", settings.page_size) or settings.page_size)
            total_pages = math.ceil(total_results / page_size) if total_results > 0 else 0
        else:
            first_payload = client.search(
                page_number=1,
                page_size=settings.page_size,
                text=query,
                extra_form_fields=extra_form_fields,
            )
            first_payload["_query"] = query

            raw_pages.append(first_payload)
            raw_result_count += len(first_payload.get("results", []) or [])
            completed_query_page_keys.add(first_page_key)
            _append_payload_jsonl(pages_path, first_payload)

            total_results = int(first_payload.get("totalResults", 0) or 0)
            page_size = int(first_payload.get("pageSize", settings.page_size) or settings.page_size)
            total_pages = math.ceil(total_results / page_size) if total_results > 0 else 0

            _write_checkpoint_state(
                state_path,
                {
                    "status": "in_progress",
                    "final_query_count": len(queries),
                    "current_query_index": query_idx,
                    "current_query_text": query,
                    "last_completed_page_number": 1,
                    "saved_page_count": len(raw_pages),
                    "saved_result_count": raw_result_count,
                },
            )

        logger.info(
            "[QUERY_COLLECTION] query=%s total_results=%s page_size=%s total_pages=%s",
            query,
            total_results,
            page_size,
            total_pages,
        )

        for page_no in range(2, total_pages + 1):
            page_key = (query, page_no)
            if page_key in completed_query_page_keys:
                logger.info("Skipping already collected page | query=%s | page=%s", query, page_no)
                continue

            payload = client.search(
                page_number=page_no,
                page_size=page_size,
                text=query,
                extra_form_fields=extra_form_fields,
            )
            payload["_query"] = query

            raw_pages.append(payload)
            raw_result_count += len(payload.get("results", []) or [])
            completed_query_page_keys.add(page_key)
            _append_payload_jsonl(pages_path, payload)

            _write_checkpoint_state(
                state_path,
                {
                    "status": "in_progress",
                    "final_query_count": len(queries),
                    "current_query_index": query_idx,
                    "current_query_text": query,
                    "last_completed_page_number": page_no,
                    "saved_page_count": len(raw_pages),
                    "saved_result_count": raw_result_count,
                },
            )

    _write_checkpoint_state(
        state_path,
        {
            "status": "complete",
            "final_query_count": len(queries),
            "current_query_index": len(queries),
            "current_query_text": queries[-1] if queries else None,
            "last_completed_page_number": None,
            "saved_page_count": len(raw_pages),
            "saved_result_count": raw_result_count,
        },
    )

    stats = ExtractionStats(
        final_query_count=len(queries),
        oversized_query_count=len(oversized_queries),
        raw_page_count=len(raw_pages),
        raw_result_count=raw_result_count,
    )

    logger.info(
        "Extraction completed | queries=%s raw_pages=%s raw_result_count=%s",
        stats.final_query_count,
        stats.raw_page_count,
        stats.raw_result_count,
    )
    return raw_pages, stats