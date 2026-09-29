from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from pipeline.mfds_regulation.collect import fetch_all, fetch_page
from pipeline.mfds_regulation.transform import (
    REGL_LIMIT_NOTE, build_silver, classify_row, match_gold)


def _kr(std, eng, reg_type="금지", cas="", syn="", notice=None, provis=None, limit=None, country="한국"):
    return {"REGULATE_TYPE": reg_type, "INGR_STD_NAME": std, "INGR_ENG_NAME": eng, "CAS_NO": cas,
            "INGR_SYNONYM": syn, "COUNTRY_NAME": country, "NOTICE_INGR_NAME": notice or std,
            "PROVIS_ATRCL": provis, "LIMIT_COND": limit}


def _gold(*rows):
    return pd.DataFrame([dict(zip(["inci_name", "kor_name", "eng_name", "kcia_cas_no"], r)) for r in rows])


AZELAIC = _kr("1,7-헵탄디카르복실산(아젤라산), 그 염류 및 유도체", "Azelaic acid, its salts and derivatives",
              cas="123-99-9", syn="Anchoic Acid, azelaic acid (INN), Nonanedioic acid (RIFM)")


def test_classify_banned_conditional_restricted():
    assert classify_row(AZELAIC) == "banned"
    assert classify_row(_kr("탤크", "Talc", provis="석면기준에 적합하지 않은 탤크")) == "conditional"
    assert classify_row(_kr("d-리모넨", "d-Limonene",
                            notice="과산화물가가 20mmol/L을 초과하는 d-리모넨")) == "conditional"
    assert classify_row(_kr("페녹시에탄올", "Phenoxyethanol", reg_type="한도", limit="1%")) == "restricted"
    assert classify_row(_kr("x", "x", reg_type="한도/금지", limit="* 배합한도 : 2%")) == "restricted"


def test_silver_keeps_only_korea_and_cleans_limit_note():
    silver = build_silver([
        _kr("토코페롤", "Tocopherol", reg_type="한도", limit="* 배합한도 :  20%"),
        _kr("아젤라익애씨드", "Azelaic acid", reg_type="한도/금지", limit="14%", country="캐나다"),
    ])
    assert len(silver) == 1
    assert silver.iloc[0]["limit_note"] == "20%"


def test_regl_korea_limit_downgrades_banned():
    raw = [_kr("녹색3호", "Fast Green FCF, CI 42053", reg_type="한도/금지"), AZELAIC]
    regl = [{"INGR_STD_NAME": "녹색3호", "INGR_ENG_NAME": "Fast Green FCF",
             "PROH_NATIONAL": "EU", "LIMIT_NATIONAL": "EU,한국"}]
    silver = build_silver(raw, regl).set_index("ingr_std_name")
    assert silver.loc["녹색3호", "kr_reg_status"] == "restricted"
    assert silver.loc["녹색3호", "limit_note"] == REGL_LIMIT_NOTE
    assert silver.iloc[1]["kr_reg_status"] == "banned"


def test_group_suffix_matches_parent_but_not_derivatives():
    silver = build_silver([AZELAIC])
    gold = _gold(("AZELAIC ACID", "아젤라익애씨드", "Azelaic Acid", ""),
                 ("POTASSIUM AZELAOYL DIGLYCINATE", "포타슘아젤로일다이글리시네이트",
                  "Potassium Azelaoyl Diglycinate", ""))
    matched, _ = match_gold(silver, gold)
    gold_rows = matched[matched["source"] == "gold"]
    assert gold_rows.set_index("inci_name")["kr_reg_status"].to_dict() == {"AZELAIC ACID": "banned"}


def test_cas_or_synonym_only_goes_to_review():
    # 원본 오류: 다이하이드라자이드 행에 아디픽애씨드의 CAS·이명이 붙어 있음
    silver = build_silver([_kr("헥센다이오익애씨드,1,6-다이하이드라자이드", "Hexanedioic Acid, 1,6-Dihydrazide",
                               cas="124-04-9", syn="adipate, adipic acid")])
    matched, review = match_gold(silver, _gold(("ADIPIC ACID", "아디픽애씨드", "Adipic Acid", "124-04-9")))
    assert "ADIPIC ACID" not in set(matched["inci_name"])
    assert review.iloc[0]["match_basis"] == "cas+synonym"


def test_banned_needs_inci_name_or_cas_when_kcia_names_are_wrong():
    silver = build_silver([
        _kr("황산o-아미노페놀", "o-Aminophenol Sulfate", cas="67845-79-8"),
        _kr("황산m-아미노페놀", "m-Aminophenol Sulfate", reg_type="한도", cas="68239-81-6", limit="2%"),
    ])
    gold = _gold(("M-AMINOPHENOL SULFATE", "황산o-아미노페놀", "o-Aminophenol Sulfate", "68239-81-6"),
                 ("M-AMINOPHENOL SULFATE", "황산m-아미노페놀", "m-Aminophenol Sulfate", "68239-81-6"))
    matched, review = match_gold(silver, gold)
    row = matched.set_index("inci_name").loc["M-AMINOPHENOL SULFATE"]
    assert (row["kr_reg_status"], row["kr_limit_note"]) == ("restricted", "2%")
    assert "황산o-아미노페놀" in review["ingr_std_name"].tolist()


def test_priority_banned_over_restricted_and_duplicate_inci_collapsed():
    silver = build_silver([
        _kr("살리실릭애씨드", "Salicylic Acid", reg_type="한도", limit="0.5%"),
        _kr("살리실릭애씨드", "Salicylic Acid", reg_type="한도", limit="0.5%"),
        _kr("탤크", "Talc", provis="석면"),
    ])
    gold = _gold(("SALICYLIC ACID", "살리실릭애씨드", "Salicylic Acid", "69-72-7"),
                 ("SALICYLIC ACID", "살리실산", "Salicylic Acid", "69-72-7"),
                 ("TALC", "탤크", "Talc", ""))
    matched = match_gold(silver, gold)[0].set_index("inci_name")
    assert matched.loc["SALICYLIC ACID", "kr_limit_note"] == "0.5%"
    assert matched.loc["SALICYLIC ACID", "kor_name"] == "살리실릭애씨드|살리실산"
    assert matched.loc["TALC", "kr_reg_status"] == "conditional"
    assert matched.loc["TALC", "kr_limit_note"] == ""


def test_regulation_names_cover_ingredients_missing_from_gold():
    # AZELAIC ACID는 KCIA Gold에 없지만 그래프에는 제품 전성분으로 들어올 수 있다.
    silver = build_silver([AZELAIC, _kr("페녹시에탄올", "Phenoxyethanol", reg_type="한도", limit="1%")])
    matched, review = match_gold(silver, _gold(("PHENOXYETHANOL", "페녹시에탄올", "Phenoxyethanol", "")))
    rows = matched.set_index("inci_name")
    assert rows.loc["AZELAIC ACID", ["source", "kr_reg_status"]].tolist() == ["mfds_name", "banned"]
    assert rows.loc["AZELAIC ACID, ITS SALTS AND DERIVATIVES", "kr_reg_status"] == "banned"
    # Gold에 있는 이름은 Gold 행으로 한 번만 나온다.
    assert rows.loc["PHENOXYETHANOL", ["source", "kr_limit_note"]].tolist() == ["gold", "1%"]
    assert not matched["inci_name"].duplicated().any()
    assert review.empty


def _payload(items, page, total):
    return {"header": {"resultCode": "00"},
            "body": {"pageNo": page, "totalCount": total, "items": [{"item": i} for i in items]}}


def test_fetch_all_pages_until_total():
    session = Mock()
    pages = {1: _payload([{"a": 1}, {"a": 2}], 1, 3), 2: _payload([{"a": 3}], 2, 3)}
    session.get.side_effect = lambda url, params, timeout: Mock(
        status_code=200, json=Mock(return_value=pages[params["pageNo"]]))
    rows, meta = fetch_all("k%2B", "rstrc", page_size=2, session=session, sleep_sec=0)
    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]
    assert meta["reported_total"] == 3 and meta["pages"] == 2
    assert session.get.call_args.kwargs["params"]["serviceKey"] == "k+"


def test_fetch_page_hides_key_on_error():
    session = Mock()
    session.get.side_effect = requests.ConnectionError("https://x/?serviceKey=secret")
    with pytest.raises(RuntimeError) as exc:
        fetch_page(session, "secret", "regl", 1)
    assert "secret" not in str(exc.value)
