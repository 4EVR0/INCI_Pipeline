"""Silver(한국 행 분류) / Gold(Ingredient 매칭) 변환. 순수 함수만 둔다.

분류 (docs/HANDOFF_regulation_and_evidence.md 4장):
- banned:      금지 또는 한도/금지, 단서조항·배합한도·조건 문구 모두 없음 → 추천 후보에서 제외
- conditional: 금지지만 단서조항 또는 고시명 조건 문구 있음(원료 품질 조건) → 정상 추천
- restricted:  한도 또는 배합한도 있음 → 추천 + kr_limit_note
한 성분에 여러 행이 걸리면 banned > restricted > conditional.
REGULATE_TYPE은 국가 공통 성분 단위 라벨이라(아젤라산 캐나다 행도 '한도/금지') 국내 판정은
한국 행의 단서조항·배합한도로 한다. 금지로 판정됐지만 regl에서 한국이 LIMIT_NATIONAL에도
있으면(예: 녹색3호, 카본블랙) 확정 금지로 보지 않고 restricted + 확인 필요 문구로 낮춘다.

매칭: 이름(영문/표준 한글명, 그룹 접미사 제거한 모물질명 포함) 정확 일치만 자동 적용.
단 banned는 INCI명 일치 또는 KCIA 영문/한글명+CAS 일치일 때만 적용한다.
CAS·이명 일치는 review로 보낸다. 원본에 다른 성분의 CAS·이명이 섞여 있다
(예: 아디픽애씨드다이하이드라자이드 행의 CAS 124-04-9, 이명 "adipic acid"는 아디픽애씨드 것).
"그 염류 및 유도체"를 다른 성분명으로 확장 매칭하지 않는다
(예: 포타슘아젤로일다이글리시네이트는 아젤라산 유도체지만 국내 사용 가능).
"""

from __future__ import annotations

import re

import pandas as pd

KOREA = "한국"
STATUS_PRIORITY = {"banned": 3, "restricted": 2, "conditional": 1}
CONDITION_PHRASES = ("초과하는", "경우에 한", "다만", "제외", "함유된", "적합하지 않은")
CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")

# 모물질명만 남기기 위해 끝에 붙은 그룹 표현만 제거한다 (확장 매칭 아님).
_GROUP_EN = re.compile(
    r"[\s,]*(?:and\s+)?(?:its|their)\s+(?:salts|esters|derivatives)"
    r"(?:\s*(?:,|and)\s*(?:salts|esters|derivatives))*\s*$", re.I)
_GROUP_KO = re.compile(
    r"[\s,]*(?:및\s*)?그\s*(?:염류|에스텔류|에스터류|유도체)"
    r"(?:\s*(?:및|,)\s*(?:염류|에스텔류|에스터류|유도체))*\s*$")
_PAREN_TAIL = re.compile(r"\s*\((?:INN|INCI|RIFM|USAN|JAN|BAN)[^)]*\)\s*$", re.I)

REGL_LIMIT_NOTE = "식약처 규제 목록에 국내 금지·한도 항목이 함께 있음(세부 조건 확인 필요)"

SILVER_COLUMNS = [
    "reg_id", "kr_reg_status", "status_reason", "regulate_type", "ingr_std_name", "ingr_eng_name",
    "cas_no", "ingr_synonym", "notice_ingr_name", "provis_atrcl", "limit_cond", "limit_note",
]


def _clean(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def norm(value: object) -> str:
    """소문자 + 영숫자·한글만."""
    return re.sub(r"[^0-9a-z가-힣]", "", _clean(value).casefold())


def cas_set(value: object) -> set[str]:
    return set(CAS_RE.findall(_clean(value)))


def limit_note(limit_cond: str) -> str:
    text = re.sub(r"^\*\s*배합한도\s*:\s*", "", limit_cond.strip())
    return re.sub(r"\s+", " ", text).strip()


def classify_row(row: dict) -> str:
    reg_type = _clean(row.get("REGULATE_TYPE"))
    provis = _clean(row.get("PROVIS_ATRCL"))
    limit = _clean(row.get("LIMIT_COND"))
    notice = _clean(row.get("NOTICE_INGR_NAME"))
    if reg_type == "한도" or limit:
        return "restricted"
    if reg_type not in {"금지", "한도/금지"}:
        raise ValueError(f"unknown REGULATE_TYPE: {reg_type!r}")
    if provis or any(phrase in notice for phrase in CONDITION_PHRASES):
        return "conditional"
    return "banned"


def korea_limit_names(regl_rows: list[dict]) -> set[str]:
    """regl에서 한국이 LIMIT_NATIONAL에 포함된 성분명(정규화) 집합."""
    names = set()
    for row in regl_rows:
        if KOREA in _clean(row.get("LIMIT_NATIONAL")):
            names |= {norm(row.get("INGR_STD_NAME")), norm(row.get("INGR_ENG_NAME"))}
    return names - {""}


def build_silver(raw_rows: list[dict], regl_rows: list[dict] | None = None) -> pd.DataFrame:
    kr_limit = korea_limit_names(regl_rows or [])
    records = []
    for idx, row in enumerate(raw_rows):
        if _clean(row.get("COUNTRY_NAME")) != KOREA:
            continue
        status = classify_row(row)
        limit = _clean(row.get("LIMIT_COND"))
        note = limit_note(limit) if limit else ""
        reason = {"banned": "금지, 조건 없음", "conditional": "금지, 단서조항/조건 문구",
                  "restricted": "한도 또는 배합한도"}[status]
        if status == "banned" and norm(row.get("INGR_STD_NAME")) in kr_limit:
            status, note, reason = "restricted", REGL_LIMIT_NOTE, "금지이나 regl 한국 LIMIT_NATIONAL 포함"
        records.append({
            "reg_id": f"kr-{idx:05d}",
            "kr_reg_status": status,
            "status_reason": reason,
            "regulate_type": _clean(row.get("REGULATE_TYPE")),
            "ingr_std_name": _clean(row.get("INGR_STD_NAME")),
            "ingr_eng_name": _clean(row.get("INGR_ENG_NAME")),
            "cas_no": _clean(row.get("CAS_NO")),
            "ingr_synonym": _clean(row.get("INGR_SYNONYM")),
            "notice_ingr_name": _clean(row.get("NOTICE_INGR_NAME")),
            "provis_atrcl": _clean(row.get("PROVIS_ATRCL")),
            "limit_cond": limit,
            "limit_note": note,
        })
    if not records:
        raise ValueError("no Korean rows in MFDS regulation snapshot")
    return pd.DataFrame(records, columns=SILVER_COLUMNS)


def primary_names(reg: dict) -> dict[str, set[str]]:
    """{"name_en": 영문명 키, "name_ko": 표준 한글명 키}"""
    names = {}
    for basis, col, group_re in (("name_en", "ingr_eng_name", _GROUP_EN),
                                 ("name_ko", "ingr_std_name", _GROUP_KO)):
        raw = _clean(reg.get(col))
        names[basis] = {norm(raw), norm(group_re.sub("", raw))} - {""}
    return names


def is_accepted(reg: dict, bases: set[str]) -> bool:
    # Gold의 KCIA 영문·한글명은 다른 성분 것이 섞인 경우가 있다
    # (예: INCI M-AMINOPHENOL SULFATE 행에 'o-Aminophenol Sulfate / 황산o-아미노페놀').
    # 금지는 INCI명 일치 또는 KCIA명+CAS 일치일 때만 적용한다.
    if reg["kr_reg_status"] == "banned":
        return "name_inci" in bases or ("cas" in bases and bool({"name_en", "name_ko"} & bases))
    return bool({"name_inci", "name_en", "name_ko"} & bases)


def synonym_names(reg: dict) -> set[str]:
    # 이명은 "\n," 또는 ", "로 구분되지만 화학명 안에도 쉼표가 있어 조각이 생길 수 있다.
    # 그래서 이명 단독 일치는 자동 적용하지 않고 review로 보낸다.
    parts = re.split(r"\n\s*,|\n|,\s+(?=[A-Za-z가-힣])", _clean(reg.get("ingr_synonym")))
    return {norm(_PAREN_TAIL.sub("", part)) for part in parts} - {""}


def regulation_inci_names(reg: dict) -> set[str]:
    """규제 행 영문명 → 후보 INCI명(대문자). 원문과 그룹 접미사를 뗀 모물질명 두 가지.

    KCIA Gold에 없는 성분도 제품 전성분을 통해 Neo4j Ingredient가 될 수 있다
    (예: AZELAIC ACID는 국내 금지라 KCIA에 없음). 규제 데이터 자체에서 INCI명을 만들어
    출력에 넣으면, 적재 스크립트가 그래프에 실제로 있는 노드와만 조인한다.
    """
    raw = _clean(reg.get("ingr_eng_name"))
    names = {re.sub(r"\s+", " ", name).strip().upper() for name in (raw, _GROUP_EN.sub("", raw))}
    return names - {""}


def match_gold(silver: pd.DataFrame, gold: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gold 성분별 국내 규제 상태와 review 목록을 반환한다.

    반환 1 (matched): inci_name, kor_name, source(gold|mfds_name), kr_reg_status, kr_limit_note,
                      match_basis, reg_ids, notice_names
    Gold에 없는 이름은 규제 행 영문명에서 만든 후보 INCI명(source=mfds_name)으로 출력한다.
    반환 2 (review):  자동 적용하지 않은 CAS/이명 일치 후보 + 금지/한도 교차검증으로 낮춘 행
    """
    required = {"inci_name", "kor_name", "eng_name", "kcia_cas_no"}
    missing = required - set(gold.columns)
    if missing:
        raise ValueError(f"Gold columns missing: {', '.join(sorted(missing))}")

    by_primary: dict[str, dict[str, list[dict]]] = {"name_en": {}, "name_ko": {}}
    by_synonym: dict[str, list[dict]] = {}
    by_cas: dict[str, list[dict]] = {}
    for reg in silver.to_dict("records"):
        for basis, keys in primary_names(reg).items():
            for key in keys:
                by_primary[basis].setdefault(key, []).append(reg)
        for key in synonym_names(reg):
            by_synonym.setdefault(key, []).append(reg)
        for cas in cas_set(reg["cas_no"]):
            by_cas.setdefault(cas, []).append(reg)

    # Neo4j Ingredient 키가 inci_name이고 Gold는 한 INCI에 한글명이 여러 행일 수 있어 inci_name 단위로 묶는다.
    groups: dict[str, dict] = {}
    for row in gold.to_dict("records"):
        inci = _clean(row.get("inci_name"))
        if not inci:
            continue
        group = groups.setdefault(inci, {"inci_name": inci, "kor_names": [], "inci": {norm(inci)},
                                         "en": set(), "ko": set(),
                                         "cas": set(), "kcia_cas_no": [], "source": "gold"})
        kor = _clean(row.get("kor_name"))
        if kor and kor not in group["kor_names"]:
            group["kor_names"].append(kor)
        group["en"] |= {norm(row.get("eng_name"))} - {""}
        group["ko"] |= {norm(kor)} - {""}
        group["cas"] |= cas_set(row.get("kcia_cas_no"))
        if _clean(row.get("kcia_cas_no")):
            group["kcia_cas_no"].append(_clean(row.get("kcia_cas_no")))

    gold_keys = {key for group in groups.values() for key in group["inci"]}
    for reg in silver.to_dict("records"):
        for inci in regulation_inci_names(reg):
            if norm(inci) in gold_keys:
                continue
            group = groups.setdefault(inci, {"inci_name": inci, "kor_names": [], "inci": {norm(inci)},
                                             "en": set(), "ko": set(), "cas": set(), "kcia_cas_no": [],
                                             "source": "mfds_name"})
            if reg["ingr_std_name"] and reg["ingr_std_name"] not in group["kor_names"]:
                group["kor_names"].append(reg["ingr_std_name"])

    matched, review = [], []
    for item in groups.values():
        kor_name = "|".join(item["kor_names"])
        hits: dict[str, dict] = {}
        bases: dict[str, set[str]] = {}

        def add(regs: list[dict], basis: str) -> None:
            for reg in regs:
                hits[reg["reg_id"]] = reg
                bases.setdefault(reg["reg_id"], set()).add(basis)

        for key in item["inci"]:
            add(by_primary["name_en"].get(key, []), "name_inci")
        for key in item["en"]:
            add(by_primary["name_en"].get(key, []), "name_en")
        for key in item["ko"]:
            add(by_primary["name_ko"].get(key, []), "name_ko")
        for key in item["inci"] | item["en"] | item["ko"]:
            add(by_synonym.get(key, []), "synonym")
        for cas in item["cas"]:
            add(by_cas.get(cas, []), "cas")

        accepted = [reg for rid, reg in hits.items() if is_accepted(reg, bases[rid])]
        for rid, reg in hits.items():
            # 규제명 후보(mfds_name)는 규제 데이터에서 만든 이름이라 review 대상이 아니다.
            if item["source"] == "gold" and (reg not in accepted or reg["limit_note"] == REGL_LIMIT_NOTE):
                review.append({
                    "inci_name": item["inci_name"],
                    "kor_name": kor_name,
                    "kcia_cas_no": "|".join(dict.fromkeys(item["kcia_cas_no"])),
                    "match_basis": "+".join(sorted(bases[rid])),
                    "reg_id": rid,
                    "kr_reg_status": reg["kr_reg_status"],
                    "ingr_std_name": reg["ingr_std_name"],
                    "ingr_eng_name": reg["ingr_eng_name"],
                    "reg_cas_no": reg["cas_no"],
                    "notice_ingr_name": reg["notice_ingr_name"],
                })
        if not accepted:
            continue
        status = max((reg["kr_reg_status"] for reg in accepted), key=STATUS_PRIORITY.__getitem__)
        notes = list(dict.fromkeys(reg["limit_note"] for reg in accepted
                                   if reg["kr_reg_status"] == "restricted" and reg["limit_note"]))
        matched.append({
            "inci_name": item["inci_name"],
            "kor_name": kor_name,
            "source": item["source"],
            "kr_reg_status": status,
            "kr_limit_note": " / ".join(notes) if status == "restricted" else "",
            "match_basis": "|".join(sorted({"+".join(sorted(bases[r["reg_id"]])) for r in accepted})),
            "reg_ids": "|".join(r["reg_id"] for r in accepted),
            "notice_names": " | ".join(dict.fromkeys(r["notice_ingr_name"] for r in accepted)),
        })
    matched_df = pd.DataFrame(matched, columns=[
        "inci_name", "kor_name", "source", "kr_reg_status", "kr_limit_note",
        "match_basis", "reg_ids", "notice_names"])
    review_df = pd.DataFrame(review, columns=[
        "inci_name", "kor_name", "kcia_cas_no", "match_basis", "reg_id", "kr_reg_status",
        "ingr_std_name", "ingr_eng_name", "reg_cas_no", "notice_ingr_name"])
    return matched_df, review_df
