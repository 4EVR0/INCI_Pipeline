"""Silver: Bronze 항목 → KCIA/CosIng Gold INCI 매칭 + 근거 규칙 적용.

산출물(항목 단위, 모든 Bronze 항목이 셋 중 하나에 들어간다):
  matched    자동 매칭 또는 사람 확정(MANUAL_INCI). partial은 자동으로 확정된 INCI만 여기 들어간다
  review     근거를 만드는 미확정 항목(needs_review·ambiguous·unmatched·partial의 나머지) → 사람이 MANUAL_INCI로 확정
  unmapped   INCI가 없는 항목(rejected, 근거가 없는 미확정). 버리지 않고 보관해 나중에 제품 전성분(한글명)과
             직접 매칭하는 입력으로 쓴다

원문 정책: Silver에는 설명 전문을 넣지 않고 효능 주장 구절(skin_claims)만 검토용으로 남긴다.

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
# 사람이 확정한 매칭. 키: (인쇄 쪽, 책 국문명) → Gold inci_name 목록. 빈 목록 = 매칭 없음으로 확정(rejected).
# 기준: 같은 식물·부위·형태이고 INCI 표기만 다르면 인정. 부위·형태가 다르거나 넓은 묶음에 붙이는 경우 거절.
MANUAL_INCI: dict[tuple[int, str], list[str]] = {
    # 1부 PDF 1~20쪽 검토 (2026-09-30)
    (12, "가공소금"): [],                                          # 표준 INCI 없음
    (12, "가시오갈피뿌리추출물"): ["ACANTHOPANAX SENTICOSUS EXTRACT"],  # Gold가 KCIA 뿌리추출물 행을 이 INCI로 묶음
    (12, "가지추출물"): [],                                        # 열매 ↔ ROOT EXTRACT 부위 다름
    (13, "갈근추출물"): [],                                        # 뿌리 ↔ FLOWER EXTRACT 부위 다름
    (14, "감자추출물"): [],                                        # 과육 ↔ CALLUS CULTURE EXTRACT 형태 다름
    (14, "감초"): ["GLYCYRRHIZA GLABRA RHIZOME/ROOT"],             # 뿌리·뿌리줄기
    (15, "감초추출물"): ["GLYCYRRHIZA INFLATA ROOT EXTRACT", "GLYCYRRHIZA URALENSIS ROOT EXTRACT",
                     "GLYCYRRHIZA GLABRA RHIZOME/ROOT EXTRACT"],   # Root ↔ Rhizome/Root 표기 차이
    (15, "감초플라보노이드"): [],                                   # 대응 INCI 없음
    (16, "검은깨추출물"): [],                                      # 추출물 ↔ SEED BUTTER 형태 다름
    (16, "겐티아나추출물"): ["GENTIANA LUTEA RHIZOME/ROOT EXTRACT"],  # 표기 차이
    (16, "겨우살이추출물"): ["VISCUM ALBUM (MISTLETOE)"],            # Gold가 KCIA 추출물 행을 이 이름으로 묶음
    (17, "고추냉이뿌리발효추출물"): [],                               # 발효물 INCI 없음
    (20, "굴추출물"): ["OSTREA EDULIS EXTRACT"],                    # KCIA 굴추출물 행의 INCI
    (30, "다시마추출물"): [],                                      # ALGAE EXTRACT(조류 전체)는 과대 적용
    (30, "달맞이꽃씨오일"): ["OENOTHERA BIENNIS OIL"],              # 달맞이꽃 오일 INCI = 씨 오일
    (31, "대나무추출물"): [],                                      # 뿌리·순 혼합 일반명, 단일 INCI 없음
    (34, "라벤더추출물"): ["LAVANDULA ANGUSTIFOLIA ANGUSTIFOLIA HERB EXTRACT"],  # herb = 지상부(전초)
    (31, "대나무수액"): ["BAMBUSA VULGARIS SAP EXTRACT"],          # 수액 ↔ SAP EXTRACT (WATER는 다른 원료일 수 있음)
    # 21~40쪽(print 35~58) 검토
    (36, "로즈마리"): [],                       # 포괄 항목 — 잎 오일/꽃 추출물 중 특정 불가
    (37, "리보플라빈"): ["LACTOFLAVIN"],         # 동일 물질의 다른 이름
    (38, "린씨드오일"): [],                     # Gold에는 에스터 유도체만 있음
    (40, "마늘추출물"): [],                     # 알뿌리 ↔ ROOT
    (41, "마시멜로뿌리추출물"): [],             # 뿌리 ↔ FLOWER
    (41, "마치현추출물"): ["PORTULACA OLERACEA FLOWER/LEAF/STEM EXTRACT"],
    (42, "마카다미아씨오일"): ["MACADAMIA INTEGRIFOLIA SEED OIL", "MACADAMIA INTEGRIFOLIA/TETRAPHYLLA SEED OIL"],
    (43, "만다린추출물"): [],                   # 열매 ↔ PEEL/다른 종
    (46, "망고씨오일"): [],                     # 오일 ↔ BUTTER
    (46, "망고추출물"): [],                     # 열매 ↔ SEED BUTTER
    (46, "매도우스위트추출물"): [],             # 후보가 ROOT
    (50, "멜론추출물"): ["CUCUMIS MELO CANTALUPENSIS FRUIT EXTRACT"],
    (50, "명일엽가루"): ["ANGELICA KEISKEI LEAF/STEM EXTRACT"],  # 가루 ↔ 추출물이나 같은 부위로 인정
    (51, "모란뿌리추출물"): [],                 # 뿌리 ↔ 가지/꽃/잎·전체 묶음
    (52, "목화씨오일"): [],                     # 종·형태 다름
    (53, "무화과나무추출물"): [],               # 후보가 BARK
    (53, "물냉이꽃/잎추출물"): ["NASTURTIUM OFFICINALE FLOWER/LEAF EXTRACT"],
    (55, "미모사잎추출물"): [],                 # 잎 ↔ BARK
    (55, "밀싹추출물"): [],                     # 싹 ↔ 전체 추출물
    (55, "밀전분"): [],                         # 후보가 귀리 전분(KCIA 매핑 오류 의심)
    (56, "밍크오일"): [],                       # 오일 ↔ 왁스
    (58, "바나나추출물"): [],                   # 열매 전체/다른 종만 있음
    # 41~60쪽(print 59~80) 검토
    (59, "바오밥나무잎추출물"): [],             # 잎 ↔ SEED
    (59, "바오밥나무펄프추출물"): [],           # 펄프 ↔ SEED
    (59, "바이오밥나무펄프추출물"): [],
    (59, "바이오플라보노이드"): ["BIOFLAVONOIDS"],
    (59, "박하가루/박하잎오일"): ["MENTHA PIPERITA LEAF EXTRACT", "MENTHA ARVENSIS LEAF EXTRACT"],
    (59, "반하추출물"): ["PINELLIA TERNATA ROOT EXTRACT"],      # 책 철자 오류(Extrat)
    (60, "발삼캐나다추출물"): ["ABIES BALSAMEA BALSAM EXTRACT"],  # 줄기에서 나는 수지(balsam)
    (60, "백금가루"): [],                       # 가루 ↔ 콜로이드
    (61, "백부자추출물"): [],
    (62, "베르가못추출물"): [],                 # 설명 식물(Monarda) ↔ INCI(Citrus) 불일치
    (62, "베어베리추출물"): ["ARCTOSTAPHYLOS UVA-URSI LEAF EXTRACT"],  # 알부틴·하이드로퀴논 → uva-ursi 잎
    (62, "베타 카로틴"): ["CI 40800"],          # 같은 물질의 색소 번호
    (63, "벤토나이트"): ["CI 77004"],           # 같은 물질의 색소 번호
    (63, "변성알코올"): [],                     # ALCOHOL DENAT. ↔ ALCOHOL
    (64, "보리추출물"): [],                     # 추출물 ↔ CERA
    (65, "복숭아추출물"): [],                   # 후보가 BUD
    (66, "부용추출물"): [],
    (66, "분꽃추출물"): [],                     # 캘러스 가루
    (67, "붓꽃추출물"): [],                     # 줄기 ↔ ROOT
    (67, "브로콜리추출물"): [],                 # 새싹 가루
    (68, "비니거"): [],                         # 책은 목초액, VINEGAR(식초)와 다른 물질
    (70, "비파엽추출물"): ["ERIOBOTRYA JAPONICA LEAF POWDER"],   # 추출물 ↔ 가루이나 같은 부위로 인정
    (70, "빈랑자추출물"): [],
    (70, "빌베리추출물"): [],
    (71, "뽕나무뿌리(상백피)추출물"): ["MORUS ALBA ROOT BARK EXTRACT"],  # 뿌리 피층 = 뿌리껍질
    (74, "사과추출물"): [],                     # 추출물 ↔ FRUIT WATER
    (76, "사탕수수추출물"): [],                 # 추출물 ↔ JUICE EXTRACT
    (76, "사프란추출물"): ["CROCUS SATIVUS EXTRACT"],            # 책 철자 오류(Sarivus)
    (76, "산딸기추출물"): [],                   # 후보가 FLOWER
    (77, "산삼배양근"): [],
    (77, "산삼배양근추출물"): [],
    (78, "살구추출물"): [],                     # 후보가 잎 세포 추출물
    (79, "상백피추출물"): [],                   # 책은 가지·껍질·잎 ↔ ROOT BARK
    (80, "생강추출물"): [],                     # 다른 종(Zingiber Aromaticus)
    # 61~80쪽(print 81~100) 검토
    (81, "서양인삼뿌리추출물"): ["PANAX QUINQUEFOLIUS ROOT EXTRACT"],  # 철자 차이
    (82, "서양자두추출물"): [],                 # 열매 추출물 ↔ JUICE/WOOD ASH
    (82, "서양측백나무추출물"): [],             # 잎·껍질 묶음
    (83, "석류나무추출물"): [],                 # 꽃·껍질·열매 묶음
    (84, "선인장추출물"): [],                   # 후보가 STEM WATER
    (85, "세라마이드 3"): ["CERAMIDE NP"],      # 같은 물질의 옛 이름
    (85, "세럼알부민"): [],
    (85, "세럼프로테인"): [],
    (86, "세신추출물"): ["ASARUM SIEBOLDII ROOT EXTRACT"],      # 철자 차이
    (86, "세이지잎수"): [],                     # 잎 증류수 ↔ 전체 추출물
    (87, "세이지잎추출물"): [],                 # 잎 ↔ 전체 추출물(넓음)
    (94, "쇠뜨기잎추출물"): ["EQUISETUM ARVENSE LEAF POWDER"],  # 추출물 ↔ 가루, 같은 부위
    (95, "쇠비름가루"): ["PORTULACA OLERACEA FLOWER/LEAF/STEM EXTRACT"],  # 가루 ↔ 추출물, 같은 부위
    (95, "수박추출물"): ["CITRULLUS LANATUS FRUIT EXTRACT"],    # Vulgaris는 Lanatus의 옛 학명
    (95, "수세미오이열매/잎/줄기추출물"): [],   # 후보가 혼합 추출물
    (96, "쉐어버터"): ["BUTYROSPERMUM PARKII BUTTER"],
    (100, "스핑고리피드"): [],                  # 후보가 CEREBROSIDES
    # 81~100쪽(print 101~121) 검토
    (102, "식물성스쿠알렌"): ["SQUALANE"],      # 수소 첨가 → 스쿠알란
    (102, "식물성오일"): ["OLUS OIL"],
    (102, "신갈나무잎추출물"): ["QUERCUS MONGOLICA LEAF EXTRACT"],  # Mongolia는 오기
    (104, "실크가루"): ["SERICA POWDER"],
    (105, "쑥추출물"): ["ARTEMISIA VULGARIS HERB EXTRACT"],    # herb = 지상부(전초)
    (109, "아르니카꽃추출물"): [],              # 추출물 ↔ 꽃 원물
    (114, "아젤라산"): [],                      # Gold에 AZELAIC ACID 없음(CAS 매칭 누락 의심)
    (114, "아카시아꽃"): [],
    (115, "안젤리카"): [],
    (116, "알로에베라"): [],                    # 후보가 VESICLES
    (117, "알로에베라잎추출물"): [],            # LEAF WATER(수증기 증류물)는 다른 원료
    (117, "알로에잎추출물"): [],                # 여러 종 묶음
    (118, "알부민"): ["ALBUMEN"],               # 달걀 흰자 알부민
    (119, "알파-비사보롤"): ["BISABOLOL"],
    (121, "애기부들이삭추출물"): ["TYPHA ANGUSTIFOLIA SPIKE EXTRACT"],
    # 101~120쪽(print 122~141) 검토
    (123, "양배추잎추출물"): [],                # 잎 추출물 ↔ JUICE/LEAF WATER
    (124, "에델바이스꽃/잎추출물"): [],         # ↔ FLOWER/LEAF WATER·ROOT EXTRACT
    (129, "연꽃뿌리추출물"): ["NELUMBO NUCIFERA ROOT POWDER"],  # 추출물 ↔ 가루, 같은 부위
    (130, "오디추출물"): [],                    # Gold에 FRUIT EXTRACT 없음(CAS 매칭 문제)
    (132, "오미자추출물"): [],                  # Gold가 CALLUS EXTRACT로 오매핑(CAS 매칭 문제)
    (134, "온천수"): [],
    (135, "올리브오일"): [],                    # Gold가 BUD EXTRACT로 오매핑(CAS 매칭 문제)
    (136, "와일드타임추출물"): ["THYMUS SERPYLLUM EXTRACT"],   # 철자(Serpillum)
    (136, "완두콩추출물"): [],                  # 전초 ↔ SEED EXTRACT
    (137, "왕대수액"): ["PHYLLOSTACHYS BAMBUSOIDES JUICE"],    # 철자(Phyllostachis)
    (139, "우유"): ["LAC"],                     # 우유의 INCI명
    (140, "월계수잎추출물"): [],                # ↔ LEAF 원물/BRANCH EXTRACT
    (141, "위치하젤 추출물"): [],               # 껍질/잔가지 항목이 별도로 있음
}


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
    "claim_scope", "effect_codes", "blocked_effects", "skin_claims", "caution", "unmapped_reason", "bronze_source",
]


def build_silver(entries: Iterable[dict], gold: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """{'matched', 'review', 'unmatched'} DataFrame. 효능은 주의 문구·정유 규칙을 적용한 뒤의 값."""
    index = build_gold_index(gold)
    out: dict[str, list[dict]] = {"matched": [], "review": [], "unmapped": []}
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
            "caution": entry.get("caution", ""), "unmapped_reason": "",
            "bronze_source": entry.get("extraction_source") or entry.get("_source", ""),
        }
        if incis:
            out["matched"].append(row)
        # 근거를 만드는 미확정 항목은 모두 검토 대상(후보가 없는 unmatched도 사람이 INCI를 찾아 확정할 수 있음).
        # partial은 자동 확정분은 matched, 나머지는 review로 보낸다.
        if status in ("partial", "needs_review", "ambiguous", "unmatched") and gives_evidence:
            out["review"].append(row)
        elif not incis:
            row["unmapped_reason"] = ("rejected_by_review" if status == "rejected"
                                      else "no_evidence_unconfirmed")
            out["unmapped"].append(row)
    return {name: pd.DataFrame(rows, columns=SILVER_COLUMNS) for name, rows in out.items()}
