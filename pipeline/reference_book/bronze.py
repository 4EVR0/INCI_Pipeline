"""Bronze: 책에서 추출한 항목(JSONL) 그대로 + 스키마 검증.

추출은 스캔 페이지 이미지를 읽어 사람이 검토 가능한 JSONL로 만드는 수동 단계다. 한 줄 = 한 항목:

  pdf_page, print_page     정수. 스캔본은 필요 없는 쪽을 뺐으므로 인쇄 쪽이 연속이 아닐 수 있다.
  kor_name                 책의 국문명
  inci_names               책의 영문(INCI)명 목록. 없으면 []
  text                     원문 설명(판독 그대로)
  roles                    화장품 용도(보습제, 산화방지제 …). '산화방지제'는 배합 안정화 용도라 피부 항산화가 아니다
  skin_claims              피부 효능 주장 원문 구절
  effect_codes             skin_claims를 그래프 Effect 코드로 옮긴 것
  claim_scope              skin(피부 명시) | general(일반 약리) | role_only(용도만) | truncated(다음 쪽으로 잘림)
  excluded_claims          섭취·전신·모발·의약(치료) 표현 등 피부 효능으로 옮기지 않은 것
  caution (선택)           자극·광독성 등 책의 주의 문구
  concern_tags, spans_pages, note, truncated (선택)
"""

from __future__ import annotations

import json
from pathlib import Path

EFFECT_CODES = frozenset({
    "ANTI_INFLAMMATORY", "SOOTHING", "BARRIER_REPAIR", "HYDRATING", "MOISTURE_RETENTION",
    "SEBUM_REGULATION", "KERATOLYTIC", "COMEDOLYTIC", "ANTIMICROBIAL", "DEPIGMENTING",
    "BRIGHTENING", "ANTIOXIDANT", "WOUND_HEALING", "ANTI_AGING", "PHOTOPROTECTIVE",
})
SCOPES = frozenset({"skin", "general", "role_only", "truncated"})
REQUIRED = ("pdf_page", "print_page", "kor_name", "inci_names", "text", "roles",
            "skin_claims", "effect_codes", "claim_scope", "excluded_claims")


def validate(entry: dict, where: str) -> list[str]:
    problems = [f"{where}: '{key}' 없음" for key in REQUIRED if key not in entry]
    if problems:
        return problems
    if entry["claim_scope"] not in SCOPES:
        problems.append(f"{where}: claim_scope '{entry['claim_scope']}'")
    unknown = set(entry["effect_codes"]) - EFFECT_CODES
    if unknown:
        problems.append(f"{where}: 알 수 없는 effect_codes {sorted(unknown)}")
    if entry["claim_scope"] == "role_only" and entry["effect_codes"]:
        problems.append(f"{where}: role_only인데 effect_codes 있음")
    if entry["effect_codes"] and not entry["skin_claims"]:
        problems.append(f"{where}: effect_codes의 근거 구절(skin_claims) 없음")
    return problems


def load_entries(paths: list[Path]) -> list[dict]:
    """여러 JSONL을 읽어 검증한다. 문제가 하나라도 있으면 전부 모아 실패."""
    entries, problems = [], []
    for path in sorted(paths):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            where = f"{path.name}:{number}"
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                problems.append(f"{where}: JSON 오류 {exc.msg}")
                continue
            problems += validate(entry, where)
            entry["_source"] = where
            entries.append(entry)
    keys = [(e.get("print_page"), e.get("kor_name")) for e in entries]
    problems += [f"중복 항목: 인쇄 {p}쪽 {k}" for (p, k) in {x for x in keys if keys.count(x) > 1}]
    if problems:
        raise ValueError("추출 항목 검증 실패:\n" + "\n".join(problems))
    return entries
