import json

import pandas as pd
import pytest

from pipeline.reference_book import run as pipeline_run
from pipeline.reference_book import silver
from pipeline.reference_book.bronze import load_entries, validate
from pipeline.reference_book.gold import build_gold


@pytest.fixture(autouse=True)
def _isolate_manual_review(monkeypatch):
    # 실제 검토 결과(MANUAL_INCI)가 가짜 테스트 데이터와 같은 키를 쓸 수 있어 비운 상태로 검증한다.
    monkeypatch.setattr(silver, "MANUAL_INCI", {})


def _entry(kor, inci, effects=(), scope="skin", **extra):
    return {"pdf_page": 1, "print_page": extra.pop("print_page", 12), "kor_name": kor, "inci_names": list(inci),
            "text": extra.pop("text", ""), "roles": [], "skin_claims": ["주장"] if effects else [],
            "effect_codes": list(effects), "claim_scope": scope, "excluded_claims": [], **extra}


GOLD = pd.DataFrame([
    {"inci_name": "GLUCOSE", "eng_name": "Glucose", "kor_name": "글루코오스"},
    {"inci_name": "GLYCYRRHIZA INFLATA ROOT EXTRACT", "eng_name": "Glycyrrhiza Inflata Root Extract", "kor_name": ""},
    # eng_name과 inci_name이 서로 다른 성분인 Gold 행(실제 사례)
    {"inci_name": "AVENA SATIVA STARCH", "eng_name": "Solanum Tuberosum (Potato) Starch", "kor_name": "감자전분"},
    {"inci_name": "SOLANUM MELONGENA ROOT EXTRACT", "eng_name": "Solanum Melongena (Eggplant) Fruit Extract",
     "kor_name": ""},
    {"inci_name": "CASSIA OBTUSIFOLIA SEED EXTRACT", "eng_name": "", "kor_name": ""},
])


def _silver(*entries):
    return {k: v.set_index("kor_name") for k, v in silver.build_silver(list(entries), GOLD).items()}


def test_silver_auto_matches_only_direct_inci():
    tables = _silver(
        _entry("글루코오스", ["Glucose"], ["MOISTURE_RETENTION"]),
        _entry("가지추출물", ["Solanum Melongena(Eggplant) Fruit Extract"], ["SOOTHING"]),
        _entry("감자전분", ["Solanum Tuberosum(Potato) Starch"], scope="role_only"),
        _entry("감초", ["Glycyrrhiza Glabra(Licorice)"], ["BRIGHTENING"]),
    )
    assert list(tables["matched"].index) == ["글루코오스"]
    # 후보가 전혀 없어도 근거가 있으면 사람이 확정할 수 있도록 검토 목록으로
    assert tables["review"].loc["감초", "match_status"] == "unmatched"
    assert tables["review"].loc["가지추출물", "review_candidates"] == "SOLANUM MELONGENA ROOT EXTRACT"
    # 근거가 없는 항목은 검토하지 않고 unmatched로
    assert tables["unmapped"].loc["감자전분", ["match_status", "unmapped_reason"]].tolist() == [
        "needs_review", "no_evidence_unconfirmed"]


def test_silver_partial_goes_to_both_matched_and_review():
    tables = _silver(_entry(
        "감초추출물", ["Glycyrrhiza Glabra(Licorice) Root Extract", "Glycyrrhiza Inflata Root Extract"], ["BRIGHTENING"]))
    assert tables["matched"].loc["감초추출물", "inci_names"] == "GLYCYRRHIZA INFLATA ROOT EXTRACT"
    assert tables["review"].loc["감초추출물", "match_status"] == "partial"
    assert tables["unmapped"].empty


def test_silver_blocks_soothing_for_caution_and_essential_oil():
    tables = _silver(
        _entry("글루코오스", ["Glucose"], ["SOOTHING", "HYDRATING"], caution="피부에 자극을 줄 수 있다"),
        _entry("결명자추출물", ["Cassia Obtusifolia Seed Extract"], ["SOOTHING", "ANTI_INFLAMMATORY", "BRIGHTENING"],
               text="수증기 증류법으로 얻은 휘발성 오일이다."),
    )
    matched = tables["matched"]
    assert matched.loc["글루코오스", ["effect_codes", "blocked_effects"]].tolist() == ["HYDRATING", "SOOTHING"]
    assert matched.loc["결명자추출물", "effect_codes"] == "BRIGHTENING"


def test_manual_rejection(monkeypatch):
    monkeypatch.setitem(silver.MANUAL_INCI, (12, "가지추출물"), [])
    tables = _silver(_entry("가지추출물", ["Solanum Melongena(Eggplant) Fruit Extract"], ["SOOTHING"]))
    assert tables["review"].empty
    assert tables["unmapped"].loc["가지추출물", ["match_status", "unmapped_reason"]].tolist() == [
        "rejected", "rejected_by_review"]


def test_manual_rejection_yields_to_exact_gold_match(monkeypatch):
    # Gold 오매핑 때문에 거절했던 항목도, Gold가 고쳐져 책 INCI가 그대로 있으면 자동 매칭된다
    monkeypatch.setitem(silver.MANUAL_INCI, (12, "글루코오스"), [])
    tables = _silver(_entry("글루코오스", ["Glucose"], ["HYDRATING"]))
    assert tables["matched"].loc["글루코오스", "inci_names"] == "GLUCOSE"


def test_manual_inci_missing_from_gold_goes_back_to_review(monkeypatch):
    monkeypatch.setitem(silver.MANUAL_INCI, (12, "감초"), ["NOT IN GOLD"])
    tables = _silver(_entry("감초", ["Glycyrrhiza Glabra(Licorice)"], ["BRIGHTENING"]))
    assert "감초" in tables["review"].index


def test_mixture_name_does_not_shadow_plain_inci():
    gold = pd.concat([GOLD, pd.DataFrame([
        {"inci_name": "(PEANUT FRUIT)/CENTELLA ASIATICA EXTRACT", "eng_name": "", "kor_name": ""},
        {"inci_name": "CENTELLA ASIATICA EXTRACT", "eng_name": "", "kor_name": ""}])])
    auto, _, status = silver.match_entry(_entry("병풀추출물", ["Centella Asiatica Extract"]), silver.build_gold_index(gold))
    assert (auto, status) == (["CENTELLA ASIATICA EXTRACT"], "matched")


def test_blemish_care_and_medical_wording_flag():
    matched = silver.build_silver([
        _entry("글루코오스", ["Glucose"], ["BLEMISH_CARE"], flags=["medical_wording"]),
        _entry("글루코오스", ["Glucose"], ["BLEMISH_CARE"], print_page=13),
        _entry("감초추출물", ["Glycyrrhiza Inflata Root Extract"], ["BLEMISH_CARE"], flags=["medical_wording"]),
    ], GOLD)["matched"]
    ev = build_gold(matched).set_index("inci_name")
    # 의약 표현이 아닌 출처가 하나라도 있으면 표시하지 않음
    assert not ev.loc["GLUCOSE", "medical_wording"]
    assert ev.loc["GLYCYRRHIZA INFLATA ROOT EXTRACT", "medical_wording"]
    assert validate(_entry("x", [], ["HYDRATING"], flags=["oops"]), "t")[0].startswith("t: 알 수 없는 flags")


def test_gold_uses_matched_evidence_only_and_prefers_skin_scope():
    matched = silver.build_silver([
        _entry("글루코오스", ["Glucose"], ["HYDRATING"], scope="general"),
        _entry("글루코오스", ["Glucose"], ["HYDRATING"], print_page=13),
        _entry("감초추출물", ["Glycyrrhiza Inflata Root Extract"], scope="role_only"),
    ], GOLD)["matched"]
    evidence = build_gold(matched)
    assert evidence[["inci_name", "effect_code", "claim_scope", "print_page"]].values.tolist() == [
        ["GLUCOSE", "HYDRATING", "skin", "13|12"]]


def test_bronze_validation():
    assert validate(_entry("x", [], ["NOT_AN_EFFECT"]), "t")[0].startswith("t: 알 수 없는")
    assert any("role_only" in p for p in validate(_entry("x", [], ["HYDRATING"], scope="role_only"), "t"))


def test_stages_end_to_end(tmp_path):
    source = tmp_path / "entries_a.jsonl"
    source.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in [
        _entry("글루코오스", ["Glucose"], ["HYDRATING"]),
        _entry("가지추출물", ["Solanum Melongena(Eggplant) Fruit Extract"], ["SOOTHING"]),
    ]), encoding="utf-8")
    gold_csv = tmp_path / "kcia.csv"
    GOLD.to_csv(gold_csv, index=False)
    root = tmp_path / "data"
    bronze = pipeline_run.run_bronze(str(source), root, "r1")
    first = json.loads((bronze / "entries.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert first["extraction_source"] == "entries_a.jsonl:1"
    silver_dir = pipeline_run.run_silver(root, "r2", gold_csv)
    matched = pd.read_csv(silver_dir / "matched.csv", dtype=str)
    assert matched["bronze_source"].tolist() == ["entries_a.jsonl:1"]
    gold_dir = pipeline_run.run_gold(root, "r3")
    evidence = pd.read_csv(gold_dir / "reference_book_evidence.csv")
    assert evidence[["inci_name", "effect_code"]].values.tolist() == [["GLUCOSE", "HYDRATING"]]
    assert json.loads((silver_dir / "metadata.json").read_text())["counts"] == {
        "matched": 1, "review": 1, "unmapped": 0}


def test_duplicate_entries_rejected(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_text("\n".join(json.dumps(_entry(k, [])) for k in ("가", "가")), encoding="utf-8")
    with pytest.raises(ValueError, match="중복 항목"):
        load_entries([path])


def test_gold_has_no_book_text_and_silver_has_no_full_description():
    tables = silver.build_silver([_entry("글루코오스", ["Glucose"], ["HYDRATING"], text="설명 전문")], GOLD)
    assert "text" not in tables["matched"].columns
    assert "skin_claims" in tables["matched"].columns   # 검토용 구절만
    evidence = build_gold(tables["matched"])
    assert not {"claims", "skin_claims", "text"} & set(evidence.columns)


def test_upload_targets_silver_and_gold_prefixes(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr("pipeline.silver_mapping.kcia_cosing.s3_io.upload_file",
                        lambda path, bucket, key: calls.append((bucket, key)) or f"s3://{bucket}/{key}")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    out = tmp_path / "run"
    out.mkdir()
    (out / "matched.csv").write_text("a\n", encoding="utf-8")
    (out / "metadata.json").write_text("{}", encoding="utf-8")
    pipeline_run._upload(out, "silver", "r1")
    assert calls == [("test-bucket", "INCI_data_silver/reference_book/run_id=r1/matched.csv"),
                     ("test-bucket", "INCI_data_silver/reference_book/run_id=r1/metadata.json")]
