"""Silver: Bronze 항목 → KCIA/CosIng Gold INCI 매칭 + 근거 규칙 적용.

산출물(항목 단위, 모든 Bronze 항목이 셋 중 하나에 들어간다):
  matched    자동 매칭 또는 사람 확정(MANUAL_INCI). partial은 자동으로 확정된 INCI만 여기 들어간다
  review     근거를 만드는 미확정 항목(needs_review·ambiguous·unmatched·partial의 나머지) → 사람이 MANUAL_INCI로 확정
  unmatched  근거가 없어 검토할 필요가 없는 미확정 항목, 사람이 매칭 없음으로 확정한 항목(rejected)

매칭(정확 일치만, 추측 금지). 책 영문명마다 따로 판단한다:
  - 자동 매칭: 책 영문명 == Gold inci_name (소문자·영숫자만, 괄호 속 일반명 제거 후 비교 포함)이고 후보가 1개
  - 검토로 보냄: Gold eng_name·kor_name으로만 찾힌 경우. Gold에는 eng_name과 inci_name이 서로 다른 성분인
    행이 있어(예: Solanum Tuberosum(Potato) Starch → AVENA SATIVA STARCH, 가지 열매 → 뿌리) 자동으로 믿지 않는다
  - 후보가 여럿이면 ambiguous. 검토 결과는 MANUAL_INCI(사람 확정)로만 반영한다.

근거 규칙:
  - role_only·truncated 항목은 근거를 만들지 않는다(CosIng 기능과 같은 약한 정보).
  - 책에 자극·광독성 등 주의 문구가 있거나 정유(휘발성 오일)이면 SOOTHING·ANTI_INFLAMMATORY 근거를 만들지 않는다
    (예: 계피유 "진정효과가 있으나 자극", 라벤더 "진정… 피부 자극성", 라벤더오일). 진정 질문에 자극·향료 성분이 올라오는 것을 막는다.
  - 근거마다 scope(skin/general)와 출처(책·인쇄 쪽), 원문 주장 구절을 남긴다.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

CAUTION_BLOCKED_EFFECTS = frozenset({"SOOTHING", "ANTI_INFLAMMATORY"})
ESSENTIAL_OIL_MARKERS = ("휘발성 오일", "정유(")
# 사람이 확정한 매칭. 키: (인쇄 쪽, 책 국문명) → Gold inci_name 목록
MANUAL_INCI: dict[tuple[int, str], list[str]] = {}


def is_essential_oil(entry: dict) -> bool:
    """정유(휘발성 오일)는 향료 알레르기·자극의 대표 원인이라 책에 주의 문구가 없어도 진정 근거로 쓰지 않는다.
    (예: 라벤더오일은 추출물 항목에만 '피부 자극성' 문구가 있고 오일 항목에는 없다.)"""
    return any(marker in entry.get("text", "") for marker in ESSENTIAL_OIL_MARKERS)


def _eng_key(name: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def _eng_key_no_paren(name: object) -> str:
    return _eng_key(re.sub(r"\([^)]*\)", "", str(name or "")))


def _kor_key(name: object) -> str:
    return re.sub(r"[\s/]", "", str(name or ""))


def build_gold_index(gold: pd.DataFrame) -> dict[tuple[str, str], set[str]]:
    index: dict[tuple[str, str], set[str]] = {}
    for row in gold.to_dict("records"):
        inci = str(row.get("inci_name") or "").strip()
        if not inci:
            continue
        for kind, value in (("inci", inci), ("eng", row.get("eng_name"))):
            if value:
                index.setdefault((kind, _eng_key(value)), set()).add(inci)
                index.setdefault((kind, _eng_key_no_paren(value)), set()).add(inci)
        if row.get("kor_name"):
            index.setdefault(("kor", _kor_key(row["kor_name"])), set()).add(inci)
    return index


def _lookup(index: dict, kind: str, name: str) -> set[str]:
    return index.get((kind, _eng_key(name))) or index.get((kind, _eng_key_no_paren(name))) or set()


def match_entry(entry: dict, index: dict[tuple[str, str], set[str]]) -> tuple[list[str], list[str], str]:
    """(자동 매칭 INCI, 검토 후보, 상태).

    상태: matched(모든 영문명 자동) | partial(일부만 자동) | needs_review(eng·kor 후보만) |
          ambiguous(INCI 후보 여럿) | unmatched | rejected(사람이 매칭 없음으로 확정)
    """
    manual = MANUAL_INCI.get((entry["print_page"], entry["kor_name"]))
    if manual is not None:
        return list(manual), [], "matched" if manual else "rejected"
    auto: set[str] = set()
    candidates: set[str] = set()
    ambiguous = False
    unresolved = 0
    for name in entry["inci_names"]:
        hits = _lookup(index, "inci", name)
        if len(hits) == 1:
            auto |= hits
            continue
        unresolved += 1
        if hits:
            ambiguous = True
            candidates |= hits
        else:
            candidates |= _lookup(index, "eng", name)
    if not entry["inci_names"] or (unresolved and not auto):
        candidates |= set().union(*(index.get(("kor", _kor_key(k)), set()) for k in entry["kor_name"].split("/")))
    candidates -= auto
    if not unresolved and auto:
        status = "matched"
    elif auto:
        status = "partial"
    elif ambiguous:
        status = "ambiguous"
    else:
        status = "needs_review" if candidates else "unmatched"
    return sorted(auto), sorted(candidates), status


SILVER_COLUMNS = [
    "print_page", "pdf_page", "kor_name", "book_inci", "match_status", "inci_names", "review_candidates",
    "claim_scope", "effect_codes", "blocked_effects", "skin_claims", "caution", "bronze_source",
]


def build_silver(entries: Iterable[dict], gold: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """{'matched', 'review', 'unmatched'} DataFrame. 효능은 주의 문구·정유 규칙을 적용한 뒤의 값."""
    index = build_gold_index(gold)
    out: dict[str, list[dict]] = {"matched": [], "review": [], "unmatched": []}
    for entry in entries:
        incis, candidates, status = match_entry(entry, index)
        effects = list(entry["effect_codes"])
        blocked = (sorted(CAUTION_BLOCKED_EFFECTS & set(effects))
                   if entry.get("caution") or is_essential_oil(entry) else [])
        effects = [e for e in effects if e not in blocked]
        gives_evidence = entry["claim_scope"] in ("skin", "general") and bool(effects)
        row = {
            "print_page": entry["print_page"], "pdf_page": entry["pdf_page"], "kor_name": entry["kor_name"],
            "book_inci": " | ".join(entry["inci_names"]), "match_status": status,
            "inci_names": " | ".join(incis), "review_candidates": " | ".join(candidates),
            "claim_scope": entry["claim_scope"], "effect_codes": "|".join(effects),
            "blocked_effects": "|".join(blocked), "skin_claims": " / ".join(entry["skin_claims"]),
            "caution": entry.get("caution", ""),
            "bronze_source": entry.get("extraction_source") or entry.get("_source", ""),
        }
        if incis:
            out["matched"].append(row)
        # 근거를 만드는 미확정 항목은 모두 검토 대상(후보가 없는 unmatched도 사람이 INCI를 찾아 확정할 수 있음).
        # partial은 자동 확정분은 matched, 나머지는 review로 보낸다.
        if status in ("partial", "needs_review", "ambiguous", "unmatched") and gives_evidence:
            out["review"].append(row)
        elif not incis:
            out["unmatched"].append(row)
    return {name: pd.DataFrame(rows, columns=SILVER_COLUMNS) for name, rows in out.items()}
