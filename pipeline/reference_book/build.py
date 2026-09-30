"""사전 항목 → Gold INCI 매칭 → 근거 후보(reference_book) 생성.

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

BOOK_CITATION = "김기연 외, 『화장품성분학 사전』, 현문사, 2011 (ISBN 9788966300891)"
EVIDENCE_TYPE = "reference_book"
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


def build(entries: Iterable[dict], gold: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """(entries 요약, evidence 후보, review) 반환."""
    index = build_gold_index(gold)
    summary, evidence, review = [], [], []
    for entry in entries:
        incis, candidates, status = match_entry(entry, index)
        effects = list(entry["effect_codes"])
        blocked = (sorted(CAUTION_BLOCKED_EFFECTS & set(effects))
                   if entry.get("caution") or is_essential_oil(entry) else [])
        effects = [e for e in effects if e not in blocked]
        gives_evidence = entry["claim_scope"] in ("skin", "general") and bool(effects)
        summary.append({
            "print_page": entry["print_page"], "kor_name": entry["kor_name"],
            "book_inci": " | ".join(entry["inci_names"]), "match_status": status,
            "inci_names": " | ".join(incis), "review_candidates": " | ".join(candidates),
            "claim_scope": entry["claim_scope"], "effect_codes": "|".join(effects),
            "blocked_by_caution": "|".join(blocked), "caution": entry.get("caution", ""),
            "source": entry.get("_source", ""),
        })
        # 근거를 만드는 항목만 검토 대상으로 낸다(용도만 있는 항목의 매칭은 근거에 쓰이지 않음).
        if status not in ("matched", "rejected") and gives_evidence:
            review.append({"print_page": entry["print_page"], "kor_name": entry["kor_name"],
                           "book_inci": " | ".join(entry["inci_names"]), "status": status,
                           "auto_matched": " | ".join(incis), "candidates": " | ".join(candidates),
                           "effect_codes": "|".join(effects)})
        if not gives_evidence:
            continue
        for inci in incis:
            for effect in effects:
                evidence.append({
                    "inci_name": inci, "effect_code": effect, "evidence_type": EVIDENCE_TYPE,
                    "claim_scope": entry["claim_scope"], "print_page": entry["print_page"],
                    "book_kor_name": entry["kor_name"], "claims": " / ".join(entry["skin_claims"]),
                    "citation": BOOK_CITATION,
                })
    evidence_df = pd.DataFrame(evidence, columns=[
        "inci_name", "effect_code", "evidence_type", "claim_scope", "print_page",
        "book_kor_name", "claims", "citation"])
    # 같은 성분·효능을 여러 항목이 말하면 skin 범위를 우선해 하나로 합친다.
    if not evidence_df.empty:
        evidence_df["_rank"] = (evidence_df["claim_scope"] != "skin").astype(int)
        evidence_df = (evidence_df.sort_values(["inci_name", "effect_code", "_rank", "print_page"])
                       .groupby(["inci_name", "effect_code"], as_index=False)
                       .agg({"evidence_type": "first", "claim_scope": "first",
                             "print_page": lambda s: "|".join(map(str, dict.fromkeys(s))),
                             "book_kor_name": lambda s: "|".join(dict.fromkeys(s)),
                             "claims": "first", "citation": "first"}))
    return pd.DataFrame(summary), evidence_df, pd.DataFrame(review, columns=[
        "print_page", "kor_name", "book_inci", "status", "auto_matched", "candidates", "effect_codes"])
