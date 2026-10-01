import pandas as pd
import pytest

from pipeline.silver_mapping.kcia_cosing.matcher import (
    CAS_REVIEW_REASON,
    cas_match,
    deduplicate_cosing,
    exact_match,
    is_cas_name_compatible,
)
from pipeline.silver_mapping.kcia_cosing.normalizer import build_name_keys, normalize_cas


def _kcia(rows):
    df = pd.DataFrame(
        [{"ingredient_code": str(i), "std_name_ko": ko, "std_name_en": en, "cas_no": cas} for i, (ko, en, cas) in enumerate(rows)]
    )
    df = pd.concat([df, build_name_keys(df["std_name_en"])], axis=1)
    df["key_cas"] = df["cas_no"].apply(normalize_cas)
    df["source"] = "kcia"
    return df


def _cosing(rows):
    df = pd.DataFrame(
        [{"substance_id": str(i), "inci_name": inci, "cas_no": cas} for i, (inci, cas) in enumerate(rows)]
    )
    for col in ["function_names", "cosmetic_restriction", "other_restrictions", "identified_ingredient",
                "status", "source", "ingest_date", "batch_id"]:
        df[col] = ""
    df = pd.concat([df, build_name_keys(df["inci_name"])], axis=1)
    df["key_cas"] = df["cas_no"].apply(normalize_cas)
    return df


# CosIng 순서상 잘못된 후보가 먼저 오도록 배치한다 (기존 drop_duplicates(key_cas)가 고르던 행).
COSING = _cosing([
    ("ALOE BARBADENSIS LEAF WATER", "85507-69-3"),
    ("ALOE BARBADENSIS LEAF EXTRACT", "85507-69-3 / 94349-62-9"),
    ("CENTELLA ASIATICA LEAF/STEM EXTRACT", "84696-21-9"),
    ("CENTELLA ASIATICA EXTRACT", "84696-21-9"),
    ("CENTELLA ASIATICA ROOT EXTRACT", "84696-21-9"),
    ("AVENA SATIVA STARCH", "9005-25-8"),
    ("SOLANUM TUBEROSUM STARCH", "9005-25-8"),
    ("SOLANUM MELONGENA ROOT EXTRACT", "84012-19-1"),
    ("SOLANUM MELONGENA FRUIT EXTRACT", "84012-19-1"),
    ("PORTULACA OLERACEA FLOWER/LEAF/STEM EXTRACT", "90083-07-1"),
    ("AQUA", "7732-18-5"),
    ("TOCOPHEROL", "1406-18-4"),
    ("AZELAIC ACID", "123-99-9"),
])


@pytest.mark.parametrize(
    "eng_name, inci_name, expected",
    [
        ("Aloe Barbadensis Leaf Extract", "ALOE BARBADENSIS LEAF WATER", False),
        ("Aloe Barbadensis Leaf Extract", "ALOE BARBADENSIS LEAF EXTRACT", True),
        ("Centella Asiatica Extract", "CENTELLA ASIATICA LEAF/STEM EXTRACT", False),
        ("Centella Asiatica Root Extract", "CENTELLA ASIATICA LEAF/STEM EXTRACT", False),
        ("Potato Starch", "AVENA SATIVA STARCH", False),
        ("Solanum Tuberosum (Potato) Starch", "SOLANUM TUBEROSUM STARCH", True),
        ("Eggplant Fruit Extract", "SOLANUM MELONGENA ROOT EXTRACT", False),
        ("Solanum Melongena (Eggplant) Fruit Extract", "SOLANUM MELONGENA ROOT EXTRACT", False),
        ("Argan Kernel Oil", "ARGANIA SPINOSA KERNEL OIL", True),
        ("Ginkgo Biloba Leaves Extract", "GINKGO BILOBA LEAF EXTRACT", True),
        # 부위·형태 토큰이 없는 일반 화합물은 동의어라도 CAS 일치만으로 승인
        ("Vitamin E", "TOCOPHEROL", True),
        ("Purified Water", "AQUA", False),
    ],
)
def test_is_cas_name_compatible(eng_name, inci_name, expected):
    assert is_cas_name_compatible(eng_name, inci_name) is expected


def test_cas_match_picks_part_form_compatible_candidate():
    kcia = _kcia([
        ("알로에베라잎추출물", "Aloe Barbadensis Leaf Extract", "85507-69-3 94349-62-9"),
        ("병풀추출물", "Centella Asiatica Extract", "84696-21-9"),
        ("병풀뿌리추출물", "Centella Asiatica Root Extract", "84696-21-9"),
        ("감자전분", "Solanum Tuberosum (Potato) Starch", "9005-25-8"),
        ("가지열매추출물", "Solanum Melongena (Eggplant) Fruit Extract", "84012-19-1"),
        ("아젤라익애씨드", "Azelaic Acid", "123-99-9"),
        ("토코페롤", "Vitamin E", "1406-18-4"),
    ])

    matched, rejected, unmatched = cas_match(kcia, COSING)
    by_ko = dict(zip(matched["std_name_ko"], matched["inci_name"]))

    assert by_ko["알로에베라잎추출물"] == "ALOE BARBADENSIS LEAF EXTRACT"
    assert by_ko["병풀추출물"] == "CENTELLA ASIATICA EXTRACT"
    assert by_ko["병풀뿌리추출물"] == "CENTELLA ASIATICA ROOT EXTRACT"
    assert by_ko["아젤라익애씨드"] == "AZELAIC ACID"
    assert by_ko["토코페롤"] == "TOCOPHEROL"
    assert set(matched["match_type"]) == {"exact_cas"}

    # 이름이 CosIng에 그대로 있는 행은 CAS 단계에서 고르지 않고 이름 매칭 단계로 넘긴다.
    assert set(unmatched["std_name_ko"]) == {"감자전분", "가지열매추출물"}
    assert rejected.empty


def test_cas_mismatch_defers_to_exact_name_match():
    kcia = _kcia([
        ("감자전분", "Solanum Tuberosum (Potato) Starch", "9005-25-8"),
        ("가지열매추출물", "Solanum Melongena (Eggplant) Fruit Extract", "84012-19-1"),
    ])

    matched_cas, _, cur = cas_match(kcia, COSING)
    assert matched_cas.empty

    cosing_full = deduplicate_cosing(COSING, "key_full")
    matched, review, _ = exact_match(cur, cosing_full, "key_full", "key_full", "exact_full_normalized")

    assert review.empty
    assert dict(zip(matched["std_name_ko"], matched["inci_name"])) == {
        "감자전분": "SOLANUM TUBEROSUM STARCH",
        "가지열매추출물": "SOLANUM MELONGENA FRUIT EXTRACT",
    }


def test_cas_mismatch_without_name_match_goes_to_review():
    kcia = _kcia([
        ("쇠비름가루", "Portulaca Oleracea Powder", "90083-07-1"),
        ("감자전분", "Potato Starch", "9005-25-8"),
        ("가지열매추출물", "Eggplant Fruit Extract", "84012-19-1"),
    ])

    matched, rejected, unmatched = cas_match(kcia, COSING)

    assert matched.empty
    assert set(unmatched["std_name_ko"]) == {"쇠비름가루", "감자전분", "가지열매추출물"}
    assert set(rejected["review_reason"]) == {CAS_REVIEW_REASON}
    assert dict(zip(rejected["std_name_ko"], rejected["candidate_inci_name"])) == {
        "쇠비름가루": "PORTULACA OLERACEA FLOWER/LEAF/STEM EXTRACT",
        "감자전분": "AVENA SATIVA STARCH",
        "가지열매추출물": "SOLANUM MELONGENA ROOT EXTRACT",
    }
