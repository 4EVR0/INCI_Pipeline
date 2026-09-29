"""식약처 화장품 규제 API 탐색 스크립트 (1회성).

- .env 의 REGULATION_API_KEY 를 읽어 3개 엔드포인트를 전체 페이징 수집한다.
- 결과: output/regulation_probe/<endpoint>.json  (output/ 은 .gitignore 대상)
- 요약: output/regulation_probe/summary.txt  (필드 목록, 건수, Azelaic Acid 검색 결과)
- 키 값은 어디에도 출력/저장하지 않는다.

실행: python scripts/probe_regulation_api.py
"""
import json
import time
import urllib.parse
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "output" / "regulation_probe"
BASE = "https://apis.data.go.kr/1471000/"
ENDPOINTS = {
    "regl": "CsmtcsReglMaterialInfoService/getCsmtcsReglMaterialInfoService",
    "rstrc": "CsmtcsUseRstrcInfoService/getCsmtcsUseRstrcInfoService",
    "rstrc_natn": "CsmtcsUseRstrcInfoService/getCsmtcsUseRstrcNatnInfoService",
}
ROWS = 100
PROBE_TERMS = ["AZELAIC", "아젤라", "NIACINAMIDE", "나이아신아마이드", "SALICYLIC", "살리실릭"]


def load_key() -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("REGULATION_API_KEY"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("REGULATION_API_KEY not found in .env")


def call(key: str, ep: str, page: int) -> dict:
    # data.go.kr 키는 Encoding/Decoding 두 형태가 있다. '%'가 있으면 이미 인코딩된 키.
    k = key if "%" in key else urllib.parse.quote(key, safe="")
    url = f"{BASE}{ep}?serviceKey={k}&pageNo={page}&numOfRows={ROWS}&type=json"
    try:
        raw = urllib.request.urlopen(url, timeout=30).read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:400]
        raise RuntimeError(f"HTTP {e.code} {e.reason} | body: {body}") from None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise RuntimeError(f"JSON 아님(키/파라미터 오류 가능): {raw[:300]}")


def items_of(resp: dict) -> tuple[list, int]:
    body = resp.get("body") or resp.get("response", {}).get("body", {})
    items = body.get("items") or []
    if isinstance(items, dict):
        items = items.get("item", [])
    if isinstance(items, dict):
        items = [items]
    items = [i.get("item", i) if isinstance(i, dict) else i for i in items]
    return items, int(body.get("totalCount") or 0)


def main() -> None:
    key = load_key()
    OUT.mkdir(parents=True, exist_ok=True)
    lines = []
    for name, ep in ENDPOINTS.items():
        try:
            first = call(key, ep, 1)
        except Exception as e:  # noqa: BLE001
            msg = str(e).replace(key, "***")
            lines.append(f"== {name}: ERROR {msg}")
            print(lines[-1])
            continue
        rows, total = items_of(first)
        pages = (total + ROWS - 1) // ROWS
        try:
            for p in range(2, pages + 1):
                rows += items_of(call(key, ep, p))[0]
                time.sleep(0.1)
        except Exception as e:  # noqa: BLE001
            lines.append(f"== {name}: 페이징 중단 at page {p}: {str(e).replace(key, '***')}")
            print(lines[-1])
        (OUT / f"{name}.json").write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
        fields = sorted({k for r in rows for k in r})
        lines.append(f"== {name}: totalCount={total}, collected={len(rows)}")
        lines.append(f"fields: {fields}")
        lines.append("sample: " + json.dumps(rows[:2], ensure_ascii=False)[:1500])
        for term in PROBE_TERMS:
            hits = [r for r in rows if term.lower() in json.dumps(r, ensure_ascii=False).lower()]
            lines.append(f"[{term}] {len(hits)} hits: " + json.dumps(hits[:3], ensure_ascii=False)[:1200])
        print(f"{name}: {len(rows)}/{total}")
        (OUT / "summary.txt").write_text("\n".join(lines), encoding="utf-8")
    (OUT / "summary.txt").write_text("\n".join(lines), encoding="utf-8")
    print(f"done → {OUT / 'summary.txt'}")


if __name__ == "__main__":
    main()
