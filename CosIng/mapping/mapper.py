from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from mapping.utils import (
    normalize_name,
    normalize_name_compact,
    normalize_cas,
    pick_existing_column,
)


@dataclass
class MappingArtifacts:
    kcia: pd.DataFrame
    cosing: pd.DataFrame
    matched_exact: pd.DataFrame
    unmatched_kcia: pd.DataFrame


class KCIACosIngMapper:
    def __init__(self, kcia_csv_path: Path, cosing_csv_path: Path):
        self.kcia_csv_path = kcia_csv_path
        self.cosing_csv_path = cosing_csv_path

    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        kcia = pd.read_csv(self.kcia_csv_path)
        cosing = pd.read_csv(self.cosing_csv_path)
        return kcia, cosing

    def prepare_data(self, kcia: pd.DataFrame, cosing: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        kcia = kcia.copy()
        cosing = cosing.copy()

        kcia_en_col = pick_existing_column(
            kcia,
            ["std_name_en", "standard_name_en", "inci_name", "ingredient_name_en"]
        )
        kcia_code_col = pick_existing_column(kcia, ["ingredient_code", "code", "id"])
        kcia_ko_col = pick_existing_column(
            kcia,
            ["std_name_ko", "standard_name_ko", "ingredient_name_ko"]
        )
        kcia_cas_col = pick_existing_column(kcia, ["cas_no", "cas", "cas_number"])

        if not kcia_en_col:
            raise ValueError("KCIA CSV에서 영문 성분명 컬럼을 찾지 못했습니다.")

        cosing_inci_col = pick_existing_column(cosing, ["inci_name", "inci name"])
        cosing_substance_id_col = pick_existing_column(cosing, ["substance_id", "substanceid"])
        cosing_cas_col = pick_existing_column(cosing, ["cas_no", "cas no", "cas"])
        cosing_function_col = pick_existing_column(cosing, ["function_names", "function", "function_name"])
        cosing_ec_col = pick_existing_column(cosing, ["ec_no", "einecs/elincs no", "ec"])

        if not cosing_inci_col:
            raise ValueError("CosIng CSV에서 INCI 컬럼을 찾지 못했습니다.")

        kcia["_kcia_code"] = kcia[kcia_code_col] if kcia_code_col else None
        kcia["_kcia_name_ko"] = kcia[kcia_ko_col] if kcia_ko_col else None
        kcia["_kcia_name_en"] = kcia[kcia_en_col]
        kcia["_kcia_name_en_norm"] = kcia["_kcia_name_en"].apply(normalize_name)
        kcia["_kcia_name_en_compact"] = kcia["_kcia_name_en"].apply(normalize_name_compact)
        kcia["_kcia_cas"] = kcia[kcia_cas_col].apply(normalize_cas) if kcia_cas_col else None

        cosing["_cosing_substance_id"] = cosing[cosing_substance_id_col] if cosing_substance_id_col else None
        cosing["_cosing_inci_name"] = cosing[cosing_inci_col]
        cosing["_cosing_inci_name_norm"] = cosing["_cosing_inci_name"].apply(normalize_name)
        cosing["_cosing_inci_name_compact"] = cosing["_cosing_inci_name"].apply(normalize_name_compact)
        cosing["_cosing_cas"] = cosing[cosing_cas_col].apply(normalize_cas) if cosing_cas_col else None
        cosing["_cosing_function"] = cosing[cosing_function_col] if cosing_function_col else None
        cosing["_cosing_ec"] = cosing[cosing_ec_col] if cosing_ec_col else None

        cosing = cosing.drop_duplicates(subset=["_cosing_inci_name_compact"], keep="first").copy()

        return kcia, cosing

    def run_exact_mapping(self) -> MappingArtifacts:
        kcia, cosing = self.load_data()
        kcia, cosing = self.prepare_data(kcia, cosing)

        matched = kcia.merge(
            cosing,
            how="left",
            left_on="_kcia_name_en_compact",
            right_on="_cosing_inci_name_compact",
            suffixes=("_kcia", "_cosing"),
        )

        matched["match_type"] = matched["_cosing_inci_name"].notna().map(
            lambda x: "name_compact_exact" if x else None
        )

        matched_exact = matched[matched["_cosing_inci_name"].notna()].copy()
        unmatched_kcia = matched[matched["_cosing_inci_name"].isna()].copy()

        return MappingArtifacts(
            kcia=kcia,
            cosing=cosing,
            matched_exact=matched_exact,
            unmatched_kcia=unmatched_kcia,
        )