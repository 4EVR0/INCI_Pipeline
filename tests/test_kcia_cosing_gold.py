import pandas as pd

from pipeline.gold_pipeline.kcia_cosing.transform import transform_to_gold


def _graphrag_row(code, inci, eng, match_type, score="100.0", is_fuzzy="False"):
    return {
        "ingredient_code": code,
        "std_name_ko": f"성분{code}",
        "std_name_en": eng,
        "kcia_cas_no": "",
        "canonical_inci_name": inci,
        "function_names": "SKIN CONDITIONING",
        "status": "Active",
        "cosmetic_restriction": "",
        "other_restrictions": "",
        "match_type": match_type,
        "match_score": score,
        "is_fuzzy": is_fuzzy,
    }


def test_gold_keeps_confirmed_and_name_exact_cas_conflict_but_drops_review_candidates():
    graphrag = pd.DataFrame([
        _graphrag_row("1", "ALOE BARBADENSIS LEAF EXTRACT", "Aloe Barbadensis Leaf Extract", "exact_cas"),
        _graphrag_row("2", "STEARETH-200", "Steareth-200", "exact_basic_cas_conflict", is_fuzzy="True"),
        _graphrag_row("3", "THYMUS VULGARIS OIL", "Thymus Vulgaris (Thyme) Oil",
                      "exact_full_normalized_cas_conflict", is_fuzzy="True"),
        _graphrag_row("4", "PORTULACA OLERACEA FLOWER/LEAF/STEM EXTRACT", "Portulaca Oleracea Powder",
                      "exact_cas_part_form_mismatch", is_fuzzy="True"),
        _graphrag_row("5", "TETRAPEPTIDE-30", "Tetrapeptide-80", "fuzzy_review_threshold", "93.3", "True"),
        _graphrag_row("6", "TRIPEPTIDE-1", "Tripeptide-31", "fuzzy_auto_cas_conflict", "96.0", "True"),
        _graphrag_row("7", "", "Unknown Ingredient", "kcia_only", ""),
    ])
    graphrag.loc[graphrag["match_type"] == "kcia_only", ["function_names", "status"]] = ""

    gold = transform_to_gold(graphrag).set_index("ingredient_code")

    assert gold.loc["1", "inci_name"] == "ALOE BARBADENSIS LEAF EXTRACT"
    assert gold.loc["2", "inci_name"] == "STEARETH-200"
    assert gold.loc["3", "inci_name"] == "THYMUS VULGARIS OIL"
    assert gold.loc["2", "cosing_functions"] == "SKIN CONDITIONING"

    # 미확정 후보는 inci_name과 CosIng 속성을 비우고 match_type으로 사유만 남긴다.
    for code, reason in [("4", "exact_cas_part_form_mismatch"), ("5", "fuzzy_review_threshold"),
                         ("6", "fuzzy_auto_cas_conflict")]:
        row = gold.loc[code]
        assert pd.isna(row["inci_name"])
        assert pd.isna(row["cosing_functions"])
        assert pd.isna(row["status"])
        assert pd.isna(row["match_score"])
        assert row["match_type"] == reason
        assert row["eng_name"]

    assert pd.isna(gold.loc["7", "inci_name"])
    assert len(gold) == 7
