from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from config import SAFE_LIMIT, DEFAULT_SEED_CHARS, DEFAULT_NEXT_CHARS
from src.utils import dump_json, load_json


class CosIngQuerySplitter:
    def __init__(
        self,
        client,
        logger,
        cache_path: Optional[Path] = None,
        seed_chars: Optional[List[str]] = None,
        next_chars: Optional[List[str]] = None,
        safe_limit: int = SAFE_LIMIT,
        extra_form_fields: Optional[Dict[str, Any]] = None,
    ):
        self.client = client
        self.logger = logger
        self.cache_path = cache_path
        self.seed_chars = seed_chars or DEFAULT_SEED_CHARS
        self.next_chars = next_chars or DEFAULT_NEXT_CHARS
        self.safe_limit = safe_limit
        self.extra_form_fields = extra_form_fields or {}
        self.count_cache: Dict[str, int] = {}

        if self.cache_path and self.cache_path.exists():
            self.count_cache = load_json(self.cache_path, default={})
            self.logger.info(
                f"[CACHE_LOADED] entries={len(self.count_cache)} path={self.cache_path}"
            )

    def save_cache(self):
        if self.cache_path:
            dump_json(self.count_cache, self.cache_path)
            self.logger.info(
                f"[CACHE_SAVED] entries={len(self.count_cache)} path={self.cache_path}"
            )

    def _cache_key(self, query: str) -> str:
        if not self.extra_form_fields:
            return query
        return f"{query}__FORM__{str(sorted(self.extra_form_fields.items()))}"

    def count_results(self, query: str) -> int:
        cache_key = self._cache_key(query)

        if cache_key in self.count_cache:
            self.logger.info(f"[CACHE_HIT] query={query} total={self.count_cache[cache_key]}")
            return int(self.count_cache[cache_key])

        payload = self.client.search(
            page_number=1,
            page_size=1,
            text=query,
            extra_form_fields=self.extra_form_fields,
        )
        total = int(payload.get("totalResults", 0) or 0)

        self.count_cache[cache_key] = total
        self.save_cache()

        return total

    def expand_query(
        self,
        prefix: str,
        depth: int = 1,
        max_depth: int = 6,
    ) -> Tuple[List[str], List[Tuple[str, int]]]:
        final_queries: List[str] = []
        oversized_queries: List[Tuple[str, int]] = []

        for ch in self.next_chars:
            new_prefix = f"{prefix}{ch}"
            q = f"{new_prefix}*"
            count = self.count_results(q)

            self.logger.info(f"[COUNT] query={q} total={count}")

            if count == 0:
                continue

            if count <= self.safe_limit:
                final_queries.append(q)
                continue

            if depth >= max_depth:
                self.logger.warning(
                    f"[OVERSIZED_AT_MAX_DEPTH] query={q} total={count} max_depth={max_depth}"
                )
                oversized_queries.append((q, count))
                continue

            sub_final, sub_oversized = self.expand_query(
                prefix=new_prefix,
                depth=depth + 1,
                max_depth=max_depth,
            )
            final_queries.extend(sub_final)
            oversized_queries.extend(sub_oversized)

        return final_queries, oversized_queries

    def build_queries(
        self,
        max_depth: int = 6,
        max_seeds: Optional[int] = None,
    ) -> Tuple[List[str], List[Tuple[str, int]]]:
        final_queries: List[str] = []
        oversized_queries: List[Tuple[str, int]] = []

        seeds = self.seed_chars[:max_seeds] if max_seeds is not None else self.seed_chars

        for seed in seeds:
            q = f"{seed}*"
            count = self.count_results(q)

            self.logger.info(f"[SEED] query={q} total={count}")

            if count == 0:
                continue

            if count <= self.safe_limit:
                final_queries.append(q)
                continue

            sub_final, sub_oversized = self.expand_query(
                prefix=seed,
                depth=1,
                max_depth=max_depth,
            )
            final_queries.extend(sub_final)
            oversized_queries.extend(sub_oversized)

        final_queries = sorted(set(final_queries))
        oversized_queries = sorted(set(oversized_queries), key=lambda x: (x[0], x[1]))

        self.logger.info(f"[QUERY_BUILD_DONE] final_query_count={len(final_queries)}")
        self.logger.info(f"[QUERY_BUILD_DONE] oversized_query_count={len(oversized_queries)}")

        return final_queries, oversized_queries