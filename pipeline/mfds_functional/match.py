"""고시 원료명(한글) → INCI명 매칭.

고시는 옛 표기(에칠, 메칠, 에텔, 디, 트리…)를 쓰고, 식약처 원료성분 DB·KCIA Gold는 현행 표준명
(에틸, 메틸, 에터, 다이, 트라이…)을 쓴다. 표기 변형을 만들어 정확 일치로만 매칭하고, 못 찾으면
unmatched로 남겨 사람이 MANUAL_INCI에 확정한다(추측 매칭 금지).

매칭 우선순위: Gold 한글명(그래프 inci_name과 같은 키) > 식약처 DB 표준명·이명의 영문명.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from pipeline.mfds_functional.extract import FunctionalIngredient

# 옛 표기 → 현행 표기 (식약처 화장품 원료 표준명 기준). 순서대로 적용.
_SPELLING = [
    ("에칠", "에틸"), ("메칠", "메틸"), ("에텔", "에터"), ("치오", "싸이오"),
    ("히드록시", "하이드록시"), ("트리", "트라이"), ("메치코", "메티코"),
]
# 사람이 원문·DB를 대조해 확정한 매칭. 키는 고시 원료명.
MANUAL_INCI: dict[str, str] = {
    # 기준고시 별표2: (-)-알파-비사보롤 97.0% 이상. 국제 INCI명은 BISABOLOL(식약처 DB 영문명은 (-)-ALPHA-BISABOLOL)
    "알파-비사보롤": "BISABOLOL",
}
# 이름이 비슷한 그래프 성분이 있지만 고시 원료와 같다고 볼 수 없어 매칭하지 않는 것(근거 과대 적용 방지).
NOT_EQUIVALENT: dict[str, str] = {
    "닥나무추출물": "고시 원료는 줄기·뿌리를 에탄올·에틸아세테이트로 추출한 특정 원료(타이로시네이즈 억제율 규격). "
                  "일반 BROUSSONETIA KAZINOKI ROOT/BARK EXTRACT와 동일시하지 않음",
    "유용성감초추출물": "고시 원료는 글라브리딘 35.0% 이상인 유용성 추출물. "
                     "일반 GLYCYRRHIZA GLABRA RHIZOME/ROOT EXTRACT와 동일시하지 않음",
}
# 기능 → 그래프 Effect 코드. 탈모는 그래프에 두피 효능이 없어 연결하지 않는다.
FUNCTION_EFFECTS: dict[str, list[str]] = {
    "whitening": ["BRIGHTENING", "DEPIGMENTING"],
    "anti_wrinkle": ["ANTI_AGING"],
    "uv_protection": ["PHOTOPROTECTIVE"],
    # [별표 4] 6: 씻어내는 제품의 "여드름성 피부 완화"라 가장 보수적인 하나만 연결
    "acne": ["COMEDOLYTIC"],
    "hair_loss": [],
}


def _key(name: object) -> str:
    return re.sub(r"[\s\-·ㆍ∙․,()]", "", str(name or "")).casefold()


def spelling_variants(name: str) -> set[str]:
    variants = {name}
    current = name
    for old, new in _SPELLING:
        current = current.replace(old, new)
        variants.add(current)
    # '디' → '다이'는 단어 첫머리·하이픈 뒤에서만 (예: 디소듐 → 다이소듐, 멘틸안트라닐레이트는 제외)
    for v in list(variants):
        variants.add(re.sub(r"(^|[-\d])디", r"\1다이", v))
    return variants


def build_index(gold: pd.DataFrame, mfds_rows: Iterable[dict]) -> dict[str, list[tuple[str, str]]]:
    """정규화 한글명 → [(inci, 출처)]."""
    index: dict[str, list[tuple[str, str]]] = {}
    for row in gold.to_dict("records"):
        inci = str(row.get("inci_name") or "").strip()
        if inci and row.get("kor_name"):
            index.setdefault(_key(row["kor_name"]), []).append((inci, "gold_kor_name"))
    for row in mfds_rows:
        eng = str(row.get("mfds_eng_name") or "").strip()
        if not eng:
            continue
        for kor in [row.get("mfds_kor_name"), *str(row.get("mfds_synonym") or "").split(",")]:
            if kor and re.search(r"[가-힣]", str(kor)):
                index.setdefault(_key(kor), []).append((eng.upper(), "mfds_db"))
    return index


def match(items: list[FunctionalIngredient], index: dict[str, list[tuple[str, str]]]) -> list[dict]:
    rows = []
    for item in items:
        names = [item.kor_name, *sorted(item.aliases)]
        hits: dict[str, str] = {}
        if item.kor_name in NOT_EQUIVALENT:
            hits = {}
        elif item.kor_name in MANUAL_INCI:
            hits = {MANUAL_INCI[item.kor_name]: "manual"}
        else:
            for name in names:
                for variant in spelling_variants(name):
                    for inci, source in index.get(_key(variant), []):
                        # Gold 매칭이 있으면 그래프 키와 같은 이름을 우선한다.
                        if inci not in hits or source == "gold_kor_name":
                            hits[inci] = source
        gold_hits = sorted(k for k, s in hits.items() if s in ("gold_kor_name", "manual"))
        chosen = gold_hits if gold_hits else sorted(hits)
        status = ("not_equivalent" if item.kor_name in NOT_EQUIVALENT
                  else "matched" if len(chosen) == 1 else "ambiguous" if chosen else "unmatched")
        rows.append({
            "kor_name": item.kor_name,
            "function": item.function,
            "inci_name": chosen[0] if len(chosen) == 1 else "",
            "match_status": status,
            "match_source": hits.get(chosen[0], "") if len(chosen) == 1 else "",
            "candidates": "|".join(chosen),
            "aliases": "|".join(sorted(item.aliases)),
            "max_content": item.max_content,
            "condition": item.condition,
            "effect_codes": "|".join(FUNCTION_EFFECTS[item.function]),
            "sources": "|".join(item.sources),
            "review_note": NOT_EQUIVALENT.get(item.kor_name, ""),
        })
    return rows
