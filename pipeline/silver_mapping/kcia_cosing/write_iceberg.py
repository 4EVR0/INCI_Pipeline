"""
Silver Mapping Iceberg write 모듈

테이블:
    matched_final  → inci_db.silver_kcia_cosing_matched_current  (overwrite)
                   → inci_db.silver_kcia_cosing_matched_history   (append)
    graphrag_map   → inci_db.silver_kcia_cosing_graphrag_current  (overwrite)
                   → inci_db.silver_kcia_cosing_graphrag_history  (append)
    fuzzy_review   → inci_db.silver_kcia_cosing_fuzzy_review_current (overwrite)

S3 위치:
    s3://{S3_BUCKET}/inci_iceberg/silver/matched/current|history/
    s3://{S3_BUCKET}/inci_iceberg/silver/graphrag/current|history/
    s3://{S3_BUCKET}/inci_iceberg/silver/fuzzy_review/current/
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    FloatType,
    NestedField,
    StringType,
    TimestamptzType,
)

from common.iceberg_config import INCIIceberg


# ==========================================
# 스키마 정의
# ==========================================

MATCHED_FINAL_SCHEMA = Schema(
    NestedField(1,  "ingredient_code",       StringType(),  required=False),
    NestedField(2,  "std_name_ko",           StringType(),  required=False),
    NestedField(3,  "std_name_en",           StringType(),  required=False),
    NestedField(4,  "old_name_ko",           StringType(),  required=False),
    NestedField(5,  "kcia_cas_no",           StringType(),  required=False),
    NestedField(6,  "canonical_inci_name",   StringType(),  required=False),
    NestedField(7,  "cosing_substance_id",   StringType(),  required=False),
    NestedField(8,  "cosing_cas_no",         StringType(),  required=False),
    NestedField(9,  "function_names",        StringType(),  required=False),
    NestedField(10, "cosmetic_restriction",  StringType(),  required=False),
    NestedField(11, "other_restrictions",    StringType(),  required=False),
    NestedField(12, "identified_ingredient", StringType(),  required=False),
    NestedField(13, "status",                StringType(),  required=False),
    NestedField(14, "match_type",            StringType(),  required=False),
    NestedField(15, "match_score",           FloatType(),   required=False),
    NestedField(16, "kcia_source",           StringType(),  required=False),
    NestedField(17, "kcia_ingest_date",      StringType(),  required=False),
    NestedField(18, "kcia_batch_id",         StringType(),  required=False),
    NestedField(19, "cosing_source",         StringType(),  required=False),
    NestedField(20, "cosing_ingest_date",    StringType(),  required=False),
    NestedField(21, "cosing_batch_id",       StringType(),  required=False),
    NestedField(22, "as_of_date",            StringType(),  required=False),
    NestedField(23, "batch_job",             StringType(),  required=False),
    NestedField(24, "batch_date",            TimestamptzType(), required=False),
)

GRAPHRAG_MAP_SCHEMA = Schema(
    NestedField(1,  "ingredient_code",       StringType(),  required=False),
    NestedField(2,  "std_name_ko",           StringType(),  required=False),
    NestedField(3,  "std_name_en",           StringType(),  required=False),
    NestedField(4,  "old_name_ko",           StringType(),  required=False),
    NestedField(5,  "kcia_cas_no",           StringType(),  required=False),
    NestedField(6,  "canonical_inci_name",   StringType(),  required=False),
    NestedField(7,  "cosing_substance_id",   StringType(),  required=False),
    NestedField(8,  "cosing_cas_no",         StringType(),  required=False),
    NestedField(9,  "function_names",        StringType(),  required=False),
    NestedField(10, "cosmetic_restriction",  StringType(),  required=False),
    NestedField(11, "other_restrictions",    StringType(),  required=False),
    NestedField(12, "identified_ingredient", StringType(),  required=False),
    NestedField(13, "status",                StringType(),  required=False),
    NestedField(14, "match_type",            StringType(),  required=False),
    NestedField(15, "match_score",           FloatType(),   required=False),
    NestedField(16, "is_fuzzy",              BooleanType(), required=False),
    NestedField(17, "batch_job",             StringType(),  required=False),
    NestedField(18, "batch_date",            TimestamptzType(), required=False),
)

FUZZY_REVIEW_SCHEMA = Schema(
    NestedField(1,  "ingredient_code",       StringType(),  required=False),
    NestedField(2,  "std_name_ko",           StringType(),  required=False),
    NestedField(3,  "std_name_en",           StringType(),  required=False),
    NestedField(4,  "old_name_ko",           StringType(),  required=False),
    NestedField(5,  "kcia_cas_no",           StringType(),  required=False),
    NestedField(6,  "candidate_inci_name",   StringType(),  required=False),
    NestedField(7,  "cosing_substance_id",   StringType(),  required=False),
    NestedField(8,  "cosing_cas_no",         StringType(),  required=False),
    NestedField(9,  "candidate_score",       FloatType(),   required=False),
    NestedField(10, "function_names",        StringType(),  required=False),
    NestedField(11, "cosmetic_restriction",  StringType(),  required=False),
    NestedField(12, "other_restrictions",    StringType(),  required=False),
    NestedField(13, "identified_ingredient", StringType(),  required=False),
    NestedField(14, "status",                StringType(),  required=False),
    NestedField(15, "review_decision",       StringType(),  required=False),
    NestedField(16, "review_reason",         StringType(),  required=False),
    NestedField(17, "fuzzy_key_review",      StringType(),  required=False),
    NestedField(18, "cosing_source",         StringType(),  required=False),
    NestedField(19, "cosing_ingest_date",    StringType(),  required=False),
    NestedField(20, "cosing_batch_id",       StringType(),  required=False),
    NestedField(21, "kcia_source",           StringType(),  required=False),
    NestedField(22, "kcia_ingest_date",      StringType(),  required=False),
    NestedField(23, "kcia_batch_id",         StringType(),  required=False),
    NestedField(24, "as_of_date",            StringType(),  required=False),
    NestedField(25, "batch_job",             StringType(),  required=False),
    NestedField(26, "batch_date",            TimestamptzType(), required=False),
)


# ==========================================
# 내부 유틸
# ==========================================

def _load_or_create_table(catalog, identifier: str, location: str, schema: Schema):
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=identifier,
            schema=schema,
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

    # 숫자형 컬럼 강제 변환 (CSV에서 읽으면 string으로 올 수 있음)
    for field in table.schema().fields:
        col = field.name
        if col not in work_df.columns:
            continue
        if isinstance(field.field_type, FloatType):
            work_df[col] = pd.to_numeric(work_df[col], errors="coerce")
        elif isinstance(field.field_type, BooleanType):
            work_df[col] = work_df[col].apply(
                lambda v: bool(v) if isinstance(v, bool) else (str(v).lower() == "true" if pd.notna(v) else None)
            )

    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        values = work_df[field.name].tolist() if field.name in work_df.columns else [None] * len(work_df)
        arrow_dict[field.name] = pa.array(values, type=field.type)

    return pa.table(arrow_dict, schema=iceberg_arrow_schema)


def _write_table_pair(
    df: pd.DataFrame,
    catalog,
    current_id: str,
    history_id: str,
    current_loc: str,
    history_loc: str,
    schema: Schema,
) -> None:
    current_table = _load_or_create_table(catalog, current_id, current_loc, schema)
    current_table.overwrite(_build_arrow_table(df, current_table))
    print(f"   Iceberg overwrite: {current_id} ({len(df)}건)")

    history_table = _load_or_create_table(catalog, history_id, history_loc, schema)
    history_table.append(_build_arrow_table(df, history_table))
    print(f"   Iceberg append:    {history_id} ({len(df)}건)")


# ==========================================
# Public API
# ==========================================

def write_silver_to_iceberg(results: dict[str, pd.DataFrame], settings) -> None:
    """
    Silver Mapping 결과를 Iceberg 테이블에 기록합니다.

    Args:
        results  : run_and_save()의 결과 dict
                   keys: matched_final, graphrag_map, fuzzy_review
        settings : silver_mapping.kcia_cosing.config.Settings 인스턴스
    """
    base = f"s3://{settings.s3_bucket}/inci_iceberg/silver"
    catalog = INCIIceberg.get_catalog()

    matched_df = results.get("matched_final")
    if matched_df is not None and not matched_df.empty:
        _write_table_pair(
            df=matched_df,
            catalog=catalog,
            current_id=INCIIceberg.SILVER_MATCHED_CURRENT_TABLE,
            history_id=INCIIceberg.SILVER_MATCHED_HISTORY_TABLE,
            current_loc=f"{base}/matched/current",
            history_loc=f"{base}/matched/history",
            schema=MATCHED_FINAL_SCHEMA,
        )
    else:
        print("   Silver matched_final: 데이터 없음 — Iceberg write 건너뜀")

    graphrag_df = results.get("graphrag_map")
    if graphrag_df is not None and not graphrag_df.empty:
        _write_table_pair(
            df=graphrag_df,
            catalog=catalog,
            current_id=INCIIceberg.SILVER_GRAPHRAG_CURRENT_TABLE,
            history_id=INCIIceberg.SILVER_GRAPHRAG_HISTORY_TABLE,
            current_loc=f"{base}/graphrag/current",
            history_loc=f"{base}/graphrag/history",
            schema=GRAPHRAG_MAP_SCHEMA,
        )
    else:
        print("   Silver graphrag_map: 데이터 없음 — Iceberg write 건너뜀")

    fuzzy_df = results.get("fuzzy_review")
    if fuzzy_df is not None and not fuzzy_df.empty:
        fuzzy_table = _load_or_create_table(
            catalog,
            INCIIceberg.SILVER_FUZZY_REVIEW_TABLE,
            f"{base}/fuzzy_review/current",
            FUZZY_REVIEW_SCHEMA,
        )
        fuzzy_table.overwrite(_build_arrow_table(fuzzy_df, fuzzy_table))
        print(f"   Iceberg overwrite: {INCIIceberg.SILVER_FUZZY_REVIEW_TABLE} ({len(fuzzy_df)}건)")
    else:
        print("   Silver fuzzy_review: 데이터 없음 — Iceberg write 건너뜀")
