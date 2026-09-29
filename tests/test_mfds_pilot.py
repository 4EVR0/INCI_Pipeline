import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from pipeline.mfds_pipeline.pilot import audit_matches, fetch_page, fetch_snapshot, load_snapshot, parse_page


def _payload(items, *, page=1, total=None):
    return {
        "response": {
            "header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE."},
            "body": {"pageNo": page, "totalCount": len(items) if total is None else total,
                     "items": {"item": items}},
        }
    }


def test_parse_page_accepts_one_or_many_items():
    row = {"INGR_KOR_NAME": "나이아신아마이드", "INGR_ENG_NAME": "NIACINAMIDE"}
    assert parse_page(_payload(row, total=1), 1) == ([row], 1)
    assert parse_page(_payload([row, row]), 1) == ([row, row], 2)


def test_parse_page_fails_on_api_error_or_wrong_page():
    payload = _payload([])
    payload["response"]["header"]["resultCode"] = "20"
    with pytest.raises(ValueError, match="resultCode=20"):
        parse_page(payload, 1)
    with pytest.raises(ValueError, match="pagination"):
        parse_page(_payload([], page=2), 1)
    with pytest.raises(ValueError, match="response must be an object"):
        parse_page([], 1)


def test_fetch_page_does_not_expose_service_key_in_request_error():
    session = Mock()
    session.get.side_effect = requests.ConnectionError("https://example/?serviceKey=secret-key")
    with pytest.raises(RuntimeError, match="MFDS request failed") as exc:
        fetch_page(session, "secret-key", 1, 100)
    assert "secret-key" not in str(exc.value)


def test_fetch_page_decodes_portal_encoded_key_once():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.return_value = _payload([], total=0)
    fetch_page(session, "a%2Fb%2Bc", 1, 100)
    assert session.get.call_args.kwargs["params"]["serviceKey"] == "a/b+c"


def test_snapshot_preserves_source_fields_and_marks_partial():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.return_value = _payload(
        [{"INGR_KOR_NAME": "판테놀", "INGR_ENG_NAME": "PANTHENOL",
          "CAS_NO": "81-13-0", "ORIGIN_MAJOR_KOR_NAME": "기원", "INGR_SYNONYM": "별칭"}],
        total=2,
    )
    rows, meta = fetch_snapshot("secret", page_size=1, max_pages=1, session=session)
    assert rows[0] == {
        "mfds_kor_name": "판테놀", "mfds_eng_name": "PANTHENOL",
        "mfds_cas_no": "81-13-0", "mfds_origin_definition": "기원",
        "mfds_synonym": "별칭",
    }
    assert meta["complete"] is False
    assert meta["reported_total"] == 2
    assert "secret" not in str(meta)


def test_snapshot_reads_all_pages():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.side_effect = [
        _payload([{"INGR_KOR_NAME": "판테놀"}], page=1, total=2),
        _payload([{"INGR_KOR_NAME": "레티놀"}], page=2, total=2),
    ]
    with patch("pipeline.mfds_pipeline.pilot.time.sleep"):
        rows, meta = fetch_snapshot("secret", page_size=1, max_pages=2, session=session)
    assert [row["mfds_kor_name"] for row in rows] == ["판테놀", "레티놀"]
    assert meta["complete"] is True


def test_snapshot_rejects_total_count_drift():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.side_effect = [
        _payload([{"INGR_KOR_NAME": "판테놀"}], page=1, total=2),
        _payload([{"INGR_KOR_NAME": "레티놀"}], page=2, total=3),
    ]
    with patch("pipeline.mfds_pipeline.pilot.time.sleep"):
        with pytest.raises(ValueError, match="totalCount changed"):
            fetch_snapshot("secret", page_size=1, max_pages=2, session=session)


def test_snapshot_rejects_duplicate_rows_across_pages():
    session = Mock()
    session.get.return_value.status_code = 200
    row = {"INGR_KOR_NAME": "판테놀"}
    session.get.return_value.json.side_effect = [
        _payload([row], page=1, total=2),
        _payload([row], page=2, total=2),
    ]
    with patch("pipeline.mfds_pipeline.pilot.time.sleep"):
        with pytest.raises(ValueError, match="duplicate rows"):
            fetch_snapshot("secret", page_size=1, max_pages=2, session=session)


def test_snapshot_rejects_page_size_above_live_api_limit():
    with pytest.raises(ValueError, match="1..500"):
        fetch_snapshot("secret", page_size=501)


def test_existing_snapshot_can_be_reaudited_without_api(tmp_path):
    (tmp_path / "mfds_ingredients.json").write_text('[{"mfds_kor_name":"판테놀"}]')
    (tmp_path / "metadata.json").write_text(json.dumps({
        "reported_total": 1, "fetched_rows": 1, "complete": True,
        "matching_audit": {"old": "discard"},
    }))
    rows, metadata = load_snapshot(tmp_path)
    assert rows[0]["mfds_kor_name"] == "판테놀"
    assert len(metadata["source_snapshot_sha256"]) == 64
    assert "matching_audit" not in metadata


def test_existing_snapshot_rejects_duplicate_rows(tmp_path):
    (tmp_path / "mfds_ingredients.json").write_text('[{"mfds_kor_name":"판테놀"},{"mfds_kor_name":"판테놀"}]')
    (tmp_path / "metadata.json").write_text(json.dumps({
        "reported_total": 2, "fetched_rows": 2, "complete": True,
    }))
    with pytest.raises(ValueError, match="duplicate rows"):
        load_snapshot(tmp_path)


def test_exact_name_and_cas_are_separate_from_conflicts_and_ambiguity():
    mfds = [
        {"mfds_kor_name": "판테놀", "mfds_eng_name": "PANTHENOL", "mfds_cas_no": "81-13-0"},
        {"mfds_kor_name": "레티놀", "mfds_eng_name": "RETINOL", "mfds_cas_no": "68-26-8"},
        {"mfds_kor_name": "중복명", "mfds_eng_name": "DUPLICATE", "mfds_cas_no": "11-11-1"},
        {"mfds_kor_name": "중복명", "mfds_eng_name": "DUPLICATE", "mfds_cas_no": "22-22-2"},
        {"mfds_kor_name": "벤조산나트륨", "mfds_eng_name": "SODIUM BENZOATE", "mfds_cas_no": "532-32-1"},
        {"mfds_kor_name": "글리세린", "mfds_eng_name": "GLYCERIN", "mfds_cas_no": ""},
        {"mfds_kor_name": "알란토인", "mfds_eng_name": "ALLANTOIN", "mfds_cas_no": ""},
        {"mfds_kor_name": "서로다른하나", "mfds_eng_name": "TWIN", "mfds_cas_no": "33-33-3"},
        {"mfds_kor_name": "서로다른둘", "mfds_eng_name": "TWIN", "mfds_cas_no": "44-44-4"},
    ]
    gold = pd.DataFrame([
        {"inci_name": "PANTHENOL", "kor_name": "판테놀", "eng_name": "", "kcia_cas_no": "81-13-0"},
        {"inci_name": "RETINOL", "kor_name": "레티놀", "eng_name": "", "kcia_cas_no": "99-99-9"},
        {"inci_name": "DUPLICATE", "kor_name": "중복명", "eng_name": "", "kcia_cas_no": ""},
        {"inci_name": "SODIUM BENZOATE", "kor_name": "중복명", "eng_name": "", "kcia_cas_no": "532-32-1"},
        {"inci_name": "GLYCERIN", "kor_name": "글리세린", "eng_name": "", "kcia_cas_no": ""},
        {"inci_name": "ALLANTOIN", "kor_name": "다른명", "eng_name": "", "kcia_cas_no": ""},
        {"inci_name": "TWIN", "kor_name": "", "eng_name": "", "kcia_cas_no": "33-33-3"},
        {"inci_name": "UNKNOWN", "kor_name": "없는 성분", "eng_name": "", "kcia_cas_no": ""},
    ])
    audit = audit_matches(mfds, gold)
    assert audit["counts"] == {
        "name_and_cas": 1, "cas_disambiguated": 1, "both_names_only": 1,
        "single_name_only": 1, "name_disagreement": 1, "cas_conflict": 1,
        "ambiguous": 1, "unmatched": 1,
    }
    with pytest.raises(ValueError, match="Gold columns missing"):
        audit_matches(mfds, gold.drop(columns="eng_name"))
