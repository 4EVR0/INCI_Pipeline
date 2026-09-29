import pandas as pd

from pipeline.mfds_functional.extract import base_name, merge, parse_annex4, parse_standard
from pipeline.mfds_functional.match import build_index, match, spelling_variants

PAD = " " * 30

STANDARD = f"""[별표 2]
{PAD}피부의 미백에 도움을 주는 기능성화장품 각조

{PAD}나이아신아마이드
{PAD} Niacinamide
         이 원료를 건조한 것은 ...
{PAD}나이아신아마이드 크림제
{PAD} Niacinamide Cream
         이 제품은 ...
[별표 4]
{PAD}디메치코디에칠벤잘말로네이트
{PAD} Dimethicodiethylbenzalmalonate
         이 원료는 ...
[별표 6]
{PAD}p-페닐렌디아민
{PAD} p-Phenylenediamine
[별표 8]
{PAD}살리실릭애씨드
{PAD} Salicylic Acid
"""

ANNEX4 = """1. 피부를 곱게 태워주거나 자외선으로부터 피부를 보호하는데 도움을 주는 제품의
  1    <삭    제>                                  <삭   제>
 26    폴리실리콘-15(디메치코디에칠벤잘말로네이트)          10 %
 28    테레프탈릴리덴디캠퍼설포닉애씨드 및 그 염류           산으로 10 %
2. 피부의 미백에 도움을 주는 제품의 성분 및 함량
  6    마그네슘아스코빌포스페이트                    3%
  7    나이아신아마이드                           2～5%
4. 모발의 색상을 변화(탈염ㆍ탈색 포함)시키는 기능을 가진 제품의 성분 및 함량
  1    p-페닐렌디아민                            2.0
6. 여드름성 피부를 완화하는데 도움을 주는 제품의 성분 및 함량
  1    살리실  릭   애씨드                          0.5 %
"""


def _items():
    return merge(parse_standard(STANDARD), parse_annex4(ANNEX4))


def test_standard_titles_strip_dosage_forms_and_skip_hair_dye():
    assert parse_standard(STANDARD) == [
        ("나이아신아마이드", "whitening", "기준고시 별표2"),
        ("나이아신아마이드", "whitening", "기준고시 별표2"),
        ("디메치코디에칠벤잘말로네이트", "uv_protection", "기준고시 별표4"),
        ("살리실릭애씨드", "acne", "기준고시 별표8"),
    ]
    assert base_name("테레프탈릴리덴디캠퍼설포닉애씨드액(33%)") == "테레프탈릴리덴디캠퍼설포닉애씨드"
    assert base_name("아데노신 침적 마스크") == "아데노신"


def test_annex4_skips_deleted_rows_and_hair_dye_and_joins_split_names():
    rows = parse_annex4(ANNEX4)
    names = [(r[0].replace(" ", ""), r[1], r[3]) for r in rows]
    assert ("살리실릭애씨드", "acne", "0.5 %") in names
    assert all("삭" not in r[0] for r in rows)
    assert all(r[1] != "hair_dye" and "페닐렌디아민" not in r[0] for r in rows)


def test_merge_combines_sources_aliases_and_keeps_annex_only_items():
    by_name = {(i.kor_name, i.function): i for i in _items()}
    niacinamide = by_name[("나이아신아마이드", "whitening")]
    assert niacinamide.max_content == "2～5%"
    assert niacinamide.sources == ["심사규정 별표4-2번 7", "기준고시 별표2"]
    polysilicone = by_name[("폴리실리콘-15", "uv_protection")]
    assert polysilicone.aliases == {"디메치코디에칠벤잘말로네이트"}
    assert "기준고시 별표4" in polysilicone.sources   # 기준고시의 이명 제목이 같은 원료로 합쳐짐
    assert ("마그네슘아스코빌포스페이트", "whitening") in by_name   # 심사규정에만 있는 원료
    assert by_name[("살리실릭애씨드", "acne")].condition == "인체세정용(씻어내는) 제품에 한정"


def test_spelling_variants_cover_old_notation():
    variants = spelling_variants("디에칠헥실부타미도트리아존")
    assert "다이에틸헥실부타미도트라이아존" in variants
    assert "멘틸안트라닐레이트" in spelling_variants("멘틸안트라닐레이트")


def test_match_prefers_gold_and_applies_manual_and_not_equivalent_rules():
    gold = pd.DataFrame([
        {"inci_name": "NIACINAMIDE", "kor_name": "나이아신아마이드"},
        {"inci_name": "POLYSILICONE-15", "kor_name": "폴리실리콘-15"},
        {"inci_name": "SALICYLIC ACID", "kor_name": "살리실릭애씨드"},
    ])
    mfds = [{"mfds_kor_name": "나이아신아마이드", "mfds_eng_name": "Niacinamide", "mfds_synonym": ""},
            {"mfds_kor_name": "마그네슘아스코빌포스페이트", "mfds_eng_name": "Magnesium Ascorbyl Phosphate",
             "mfds_synonym": ""}]
    from pipeline.mfds_functional.extract import FunctionalIngredient
    items = _items() + [FunctionalIngredient("알파-비사보롤", "whitening"),
                        FunctionalIngredient("닥나무추출물", "whitening")]
    rows = {r["kor_name"]: r for r in match(items, build_index(gold, mfds))}
    assert (rows["나이아신아마이드"]["inci_name"], rows["나이아신아마이드"]["match_source"]) == (
        "NIACINAMIDE", "gold_kor_name")
    assert rows["마그네슘아스코빌포스페이트"]["inci_name"] == "MAGNESIUM ASCORBYL PHOSPHATE"
    assert (rows["알파-비사보롤"]["inci_name"], rows["알파-비사보롤"]["match_source"]) == ("BISABOLOL", "manual")
    assert (rows["닥나무추출물"]["match_status"], rows["닥나무추출물"]["inci_name"]) == ("not_equivalent", "")
    assert rows["나이아신아마이드"]["effect_codes"] == "BRIGHTENING|DEPIGMENTING"
    assert rows["살리실릭애씨드"]["effect_codes"] == "COMEDOLYTIC"
