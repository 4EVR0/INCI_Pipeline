"""Bronze: 식약처 화장품 규제 API 전체 페이징 수집 (원본 필드 그대로 보존).

- rstrc: 사용제한 원료정보. REGULATE_TYPE, INGR_STD_NAME, INGR_ENG_NAME, CAS_NO, INGR_SYNONYM,
         COUNTRY_NAME, NOTICE_INGR_NAME, PROVIS_ATRCL, LIMIT_COND
- regl:  규제 원료정보. INGR_STD_NAME, INGR_ENG_NAME, PROH_NATIONAL, LIMIT_NATIONAL
         (rstrc 금지 판정 교차검증용)
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from urllib.parse import unquote

import requests

from pipeline.mfds_pipeline.pilot import _session, parse_page

BASE_URL = "https://apis.data.go.kr/1471000/"
ENDPOINTS = {
    "rstrc": "CsmtcsUseRstrcInfoService/getCsmtcsUseRstrcInfoService",
    "regl": "CsmtcsReglMaterialInfoService/getCsmtcsReglMaterialInfoService",
}
PAGE_SIZE = 100


def fetch_page(session: requests.Session, service_key: str, endpoint: str, page: int,
               page_size: int = PAGE_SIZE) -> tuple[list[dict], int]:
    if not service_key:
        raise ValueError("REGULATION_API_KEY is required")
    # requests가 쿼리를 인코딩하므로 포털의 인코딩 키는 한 번 디코딩해서 넘긴다.
    try:
        response = session.get(
            BASE_URL + ENDPOINTS[endpoint],
            params={"serviceKey": unquote(service_key), "pageNo": page,
                    "numOfRows": page_size, "type": "json"},
            timeout=30,
        )
    except requests.RequestException:
        # 예외 메시지에 serviceKey가 포함된 URL이 들어갈 수 있다.
        raise RuntimeError("MFDS regulation request failed") from None
    if response.status_code != 200:
        raise RuntimeError(f"MFDS regulation HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError:
        raise ValueError("MFDS regulation API returned non-JSON data") from None
    rows, total = parse_page(payload, page)
    # 이 서비스는 items가 [{"item": {...}}, ...] 형태로 올 수 있다.
    return [row["item"] if isinstance(row.get("item"), dict) else row for row in rows], total


def fetch_all(service_key: str, endpoint: str, *, page_size: int = PAGE_SIZE,
              session: requests.Session | None = None,
              sleep_sec: float = 0.1) -> tuple[list[dict], dict]:
    own_session = session is None
    session = session or _session()
    rows: list[dict] = []
    total: int | None = None
    page = 0
    try:
        while total is None or len(rows) < total:
            page += 1
            batch, reported = fetch_page(session, service_key, endpoint, page, page_size)
            if total is None:
                total = reported
            elif reported != total:
                raise ValueError("MFDS regulation totalCount changed during pagination")
            if not batch:
                break
            rows.extend(batch)
            time.sleep(sleep_sec)
    finally:
        if own_session:
            session.close()
    if total is None or len(rows) != total:
        raise ValueError(f"MFDS regulation incomplete: fetched {len(rows)} of {total}")
    return rows, {
        "endpoint": endpoint,
        "api_url": BASE_URL + ENDPOINTS[endpoint],
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "reported_total": total,
        "fetched_rows": len(rows),
        "pages": page,
        "page_size": page_size,
    }
