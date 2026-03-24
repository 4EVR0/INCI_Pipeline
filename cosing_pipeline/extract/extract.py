import math
from typing import Any, Dict, List, Optional, Tuple

from cosing_pipeline.extract.client import CosIngClient
from cosing_pipeline.extract.splitter import CosIngQuerySplitter
from cosing_pipeline.models import ExtractionStats
from cosing_pipeline.utils.logging_utils import setup_logger


logger = setup_logger(__name__)


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

    raw_pages: List[Dict[str, Any]] = []
    raw_result_count = 0

    for query in queries:
        logger.info("Collecting query=%s", query)
        first_payload = client.search(
            page_number=1,
            page_size=settings.page_size,
            text=query,
            extra_form_fields=extra_form_fields,
        )

        total_results = int(first_payload.get("totalResults", 0) or 0)
        page_size = int(first_payload.get("pageSize", settings.page_size) or settings.page_size)
        total_pages = math.ceil(total_results / page_size) if total_results > 0 else 0

        first_payload["_query"] = query
        raw_pages.append(first_payload)
        raw_result_count += len(first_payload.get("results", []) or [])

        logger.info(
            "[QUERY_COLLECTION] query=%s total_results=%s page_size=%s total_pages=%s",
            query,
            total_results,
            page_size,
            total_pages,
        )

        for page_no in range(2, total_pages + 1):
            payload = client.search(
                page_number=page_no,
                page_size=page_size,
                text=query,
                extra_form_fields=extra_form_fields,
            )
            payload["_query"] = query
            raw_pages.append(payload)
            raw_result_count += len(payload.get("results", []) or [])

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
