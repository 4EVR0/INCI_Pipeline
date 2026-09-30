import json

import pandas as pd
import pytest

from pipeline.reference_book import build as book
from pipeline.reference_book.entries import load_entries, validate


def _entry(kor, inci, effects=(), scope="skin", **extra):
    return {"pdf_page": 1, "print_page": 12, "kor_name": kor, "inci_names": list(inci), "text": extra.pop("text", ""),
            "roles": [], "skin_claims": ["주장"] if effects else [], "effect_codes": list(effects),
            "claim_scope": scope, "excluded_claims": [], **extra}


GOLD = pd.DataFrame([
    {"inci_name": "GLUCOSE", "eng_name": "Glucose", "kor_name": "글루코오스"},
    {"inci_name": "GLYCYRRHIZA INFLATA ROOT EXTRACT", "eng_name": "Glycyrrhiza Inflata Root Extract", "kor_name": ""},
    # eng_name과 inci_name이 서로 다른 성분인 Gold 행(실제 사례)
    {"inci_name": "AVENA SATIVA STARCH", "eng_name": "Solanum Tuberosum (Potato) Starch", "kor_name": "감자전분"},
    {"inci_name": "SOLANUM MELONGENA ROOT EXTRACT", "eng_name": "Solanum Melongena (Eggplant) Fruit Extract",
     "kor_name": ""},
    {"inci_name": "CASSIA OBTUSIFOLIA SEED EXTRACT", "eng_name": "", "kor_name": ""},
])


def _build(*entries):
    summary, evidence, review = book.build(list(entries), GOLD)
    return summary.set_index("kor_name"), evidence, review.set_index("kor_name")


def test_only_direct_inci_matches_are_automatic():
    summary, evidence, review = _build(
        _entry("글루코오스", ["Glucose"], ["MOISTURE_RETENTION"]),
        _entry("가지추출물", ["Solanum Melongena(Eggplant) Fruit Extract"], ["SOOTHING"]),
    )
    assert summary.loc["글루코오스", "match_status"] == "matched"
    assert summary.loc["가지추출물", "match_status"] == "needs_review"
    assert review.loc["가지추출물", "candidates"] == "SOLANUM MELONGENA ROOT EXTRACT"
    assert set(evidence["inci_name"]) == {"GLUCOSE"}


def test_each_book_name_is_matched_independently():
    summary, evidence, review = _build(_entry(
        "감초추출물", ["Glycyrrhiza Glabra(Licorice) Root Extract", "Glycyrrhiza Inflata Root Extract"], ["BRIGHTENING"]))
    assert summary.loc["감초추출물", "match_status"] == "partial"
    assert list(evidence["inci_name"]) == ["GLYCYRRHIZA INFLATA ROOT EXTRACT"]
    assert "감초추출물" in review.index


def test_caution_and_essential_oil_block_soothing_but_keep_other_effects():
    _, evidence, _ = _build(
        _entry("글루코오스", ["Glucose"], ["SOOTHING", "HYDRATING"], caution="피부에 자극을 줄 수 있다"),
        _entry("결명자추출물", ["Cassia Obtusifolia Seed Extract"], ["SOOTHING", "ANTI_INFLAMMATORY", "BRIGHTENING"],
               text="수증기 증류법으로 얻은 휘발성 오일이다."),
    )
    assert set(map(tuple, evidence[["inci_name", "effect_code"]].values)) == {
        ("GLUCOSE", "HYDRATING"), ("CASSIA OBTUSIFOLIA SEED EXTRACT", "BRIGHTENING")}


def test_role_only_entries_give_no_evidence_or_review():
    summary, evidence, review = _build(_entry("감자전분", ["Solanum Tuberosum(Potato) Starch"], scope="role_only"))
    assert evidence.empty and review.empty
    assert summary.loc["감자전분", "match_status"] == "needs_review"


def test_manual_mapping_overrides(monkeypatch):
    monkeypatch.setitem(book.MANUAL_INCI, (12, "가지추출물"), [])
    summary, evidence, review = _build(
        _entry("가지추출물", ["Solanum Melongena(Eggplant) Fruit Extract"], ["SOOTHING"]))
    assert summary.loc["가지추출물", "match_status"] == "rejected"
    assert evidence.empty and review.empty


def test_duplicate_evidence_prefers_skin_scope():
    a = _entry("글루코오스", ["Glucose"], ["HYDRATING"], scope="general")
    b = dict(_entry("글루코오스", ["Glucose"], ["HYDRATING"]), print_page=13)
    _, evidence, _ = _build(a, b)
    assert len(evidence) == 1
    assert evidence.iloc[0]["claim_scope"] == "skin"
    assert evidence.iloc[0]["print_page"] == "13|12"


def test_validation_rejects_bad_entries(tmp_path):
    assert validate(_entry("x", [], ["NOT_AN_EFFECT"]), "t")[0].startswith("t: 알 수 없는")
    assert any("role_only" in p for p in validate(_entry("x", [], ["HYDRATING"], scope="role_only"), "t"))
    path = tmp_path / "a.jsonl"
    path.write_text("\n".join(json.dumps(_entry(k, [])) for k in ("가", "가")), encoding="utf-8")
    with pytest.raises(ValueError, match="중복 항목"):
        load_entries([path])
