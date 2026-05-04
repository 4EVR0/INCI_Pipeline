"""
Gold Ingredients Iceberg write 모듈

테이블:
    inci_db.gold_kcia_cosing_ingredients_current  — overwrite (최신 배치만 유지)
    inci_db.gold_kcia_cosing_ingredients_history  — append    (누적 이력 보관)

S3 위치:
    s3://{S3_BUCKET}/inci_iceberg/gold/ingredients/current/
    s3://{S3_BUCKET}/inci_iceberg/gold/ingredients/history/
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, FloatType, NestedField, StringType, TimestamptzType

from common.iceberg_config import INCIIceberg


# ==========================================
# Gold Ingredients 스키마
# ==========================================

GOLD_INGREDIENTS_SCHEMA = Schema(
    NestedField(1,  "inci_name",            StringType(),      required=False),
    NestedField(2,  "kor_name",             StringType(),      required=False),
    NestedField(3,  "eng_name",             StringType(),      required=False),
    NestedField(4,  "ingredient_code",      StringType(),      required=False),
    NestedField(5,  "kcia_cas_no",          StringType(),      required=False),
    NestedField(6,  "cosing_functions",     StringType(),      required=False),
    NestedField(7,  "status",               StringType(),      required=False),
    NestedField(8,  "cosmetic_restriction", StringType(),      required=False),
    NestedField(9,  "other_restrictions",   StringType(),      required=False),
    NestedField(10, "match_type",           StringType(),      required=False),
    NestedField(11, "match_score",          FloatType(),       required=False),
    NestedField(12, "is_fuzzy",             BooleanType(),     required=False),
    NestedField(13, "batch_job",            StringType(),      required=False),
    NestedField(14, "batch_date",           TimestamptzType(), required=False),
)


# ==========================================
# 내부 유틸
# ==========================================

def _load_or_create_table(catalog, identifier: str, location: str):
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=identifier,
            schema=GOLD_INGREDIENTS_SCHEMA,
            location=location,
            partition_spec=PartitionSpec(),
        )


def _build_arrow_table(df: pd.DataFrame, table) -> pa.Table:
    iceberg_arrow_schema = table.schema().as_arrow()

    work_df = df.copy()
    for col in iceberg_arrow_schema.names:
        if col not in work_df.columns:
            work_df[col] = None

    if "batch_date" in work_df.columns:
        work_df["batch_date"] = pd.to_datetime(work_df["batch_date"], utc=True, errors="coerce")
    if "match_score" in work_df.columns:
        work_df["match_score"] = pd.to_numeric(work_df["match_score"], errors="coerce")
    if "is_fuzzy" in work_df.columns:
        work_df["is_fuzzy"] = work_df["is_fuzzy"].apply(
            lambda v: bool(v) if isinstance(v, bool) else (str(v).lower() == "true" if pd.notna(v) else None)
        )

    # pd.NA → None 변환 (pyarrow 직렬화 호환)
    work_df = work_df.where(work_df.notna(), other=None)

    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        values = work_df[field.name].tolist() if field.name in work_df.columns else [None] * len(work_df)
        arrow_dict[field.name] = pa.array(values, type=field.type)

    return pa.table(arrow_dict, schema=iceberg_arrow_schema)


# ==========================================
# Public API
# ==========================================

def write_gold_to_iceberg(gold_df: pd.DataFrame, settings) -> None:
    """
    Gold ingredients 데이터를 Iceberg 테이블에 기록합니다.

    Args:
        gold_df  : transform_matched_final_to_gold() 반환값 (pd.DataFrame)
        settings : gold_pipeline.kcia_cosing.config.GoldSettings 인스턴스
    """
    if gold_df.empty:
        print("   Gold ingredients: 데이터 없음 — Iceberg write 건너뜀")
        return

    base = f"s3://{settings.s3_bucket}/inci_iceberg/gold"
    catalog = INCIIceberg.get_catalog()

    # current (overwrite)
    current_table = _load_or_create_table(
        catalog,
        INCIIceberg.GOLD_INGREDIENTS_CURRENT_TABLE,
        f"{base}/ingredients/current",
    )
    current_table.overwrite(_build_arrow_table(gold_df, current_table))
    print(f"   Iceberg overwrite: {INCIIceberg.GOLD_INGREDIENTS_CURRENT_TABLE} ({len(gold_df)}건)")

    # history (append)
    history_table = _load_or_create_table(
        catalog,
        INCIIceberg.GOLD_INGREDIENTS_HISTORY_TABLE,
        f"{base}/ingredients/history",
    )
    history_table.append(_build_arrow_table(gold_df, history_table))
    print(f"   Iceberg append:    {INCIIceberg.GOLD_INGREDIENTS_HISTORY_TABLE} ({len(gold_df)}건)")
