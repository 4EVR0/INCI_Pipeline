"""
CosIng Bronze Iceberg write 모듈

테이블:
    inci_db.cosing_bronze_current  — overwrite (최신 배치만 유지)
    inci_db.cosing_bronze_history  — append    (누적 이력 보관)

S3 위치:
    s3://{S3_BUCKET}/inci_iceberg/cosing_bronze/current/
    s3://{S3_BUCKET}/inci_iceberg/cosing_bronze/history/
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import (
    FloatType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

from common.iceberg_config import INCIIceberg

# ==========================================
# CosIng Bronze 스키마
# ==========================================

COSING_BRONZE_SCHEMA = Schema(
    # 성분 식별
    NestedField(1,  "reference_uuid",              StringType(),  required=False),
    NestedField(2,  "database",                    StringType(),  required=False),
    NestedField(3,  "database_label",              StringType(),  required=False),
    NestedField(4,  "language",                    StringType(),  required=False),
    NestedField(5,  "content_type",                StringType(),  required=False),
    NestedField(6,  "weight",                      FloatType(),   required=False),
    NestedField(7,  "substance_id",                StringType(),  required=True),
    NestedField(8,  "inci_name",                   StringType(),  required=False),
    NestedField(9,  "common_ingredient_name",      StringType(),  required=False),
    NestedField(10, "inn_name",                    StringType(),  required=False),
    NestedField(11, "inci_usa_name",               StringType(),  required=False),
    NestedField(12, "chemical_name",               StringType(),  required=False),
    NestedField(13, "chemical_description",        StringType(),  required=False),
    NestedField(14, "cas_no",                      StringType(),  required=False),
    NestedField(15, "ec_no",                       StringType(),  required=False),
    # 기능 / 규제
    NestedField(16, "function_names",              StringType(),  required=False),
    NestedField(17, "function_count",              IntegerType(), required=False),
    NestedField(18, "cosmetic_restriction",        StringType(),  required=False),
    NestedField(19, "other_restrictions",          StringType(),  required=False),
    NestedField(20, "maximum_concentration",       StringType(),  required=False),
    NestedField(21, "annex_no",                    StringType(),  required=False),
    NestedField(22, "identified_ingredient",       StringType(),  required=False),
    NestedField(23, "classification_information",  StringType(),  required=False),
    # 상태 / 버전
    NestedField(24, "item_type",                   StringType(),  required=False),
    NestedField(25, "status",                      StringType(),  required=False),
    NestedField(26, "official_journal_publication",StringType(),  required=False),
    NestedField(27, "perfuming",                   StringType(),  required=False),
    NestedField(28, "ph_eur_name",                 StringType(),  required=False),
    NestedField(29, "es_ingest_date",              StringType(),  required=False),
    NestedField(30, "es_queue_date",               StringType(),  required=False),
    NestedField(31, "current_version",             StringType(),  required=False),
    NestedField(32, "corporate_search_version",    StringType(),  required=False),
    NestedField(33, "raw_metadata",                StringType(),  required=False),
    # API 응답 메타
    NestedField(34, "query_text",                  StringType(),  required=False),
    NestedField(35, "api_version",                 StringType(),  required=False),
    NestedField(36, "terms",                       StringType(),  required=False),
    NestedField(37, "response_time",               FloatType(),   required=False),
    NestedField(38, "page_number",                 IntegerType(), required=False),
    NestedField(39, "page_size",                   IntegerType(), required=False),
    NestedField(40, "total_results",               IntegerType(), required=False),
    NestedField(41, "sort",                        StringType(),  required=False),
    # 배치 공통
    NestedField(42, "source",                      StringType(),  required=False),
    NestedField(43, "ingest_date",                 StringType(),  required=False),
    NestedField(44, "batch_month",                 StringType(),  required=False),
    NestedField(45, "batch_id",                    StringType(),  required=False),
    NestedField(46, "batch_job",                   StringType(),  required=False),
    NestedField(47, "batch_date",                  TimestamptzType(), required=False),
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
            schema=COSING_BRONZE_SCHEMA,
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

    # raw_metadata가 dict로 남아있으면 JSON 문자열로 변환
    if "raw_metadata" in work_df.columns:
        import json
        work_df["raw_metadata"] = work_df["raw_metadata"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
        )

    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        values = work_df[field.name].tolist() if field.name in work_df.columns else [None] * len(work_df)
        arrow_dict[field.name] = pa.array(values, type=field.type)

    return pa.table(arrow_dict, schema=iceberg_arrow_schema)


# ==========================================
# Public API
# ==========================================

def write_cosing_bronze_to_iceberg(df: pd.DataFrame, settings) -> None:
    """
    CosIng Bronze 데이터를 Iceberg 테이블에 기록합니다.

    Args:
        df       : transform_to_bronze() 반환값 (pd.DataFrame)
        settings : cosing_pipeline.config.Settings 인스턴스
    """
    if df.empty:
        print("   CosIng Bronze: 데이터 없음 — Iceberg write 건너뜀")
        return

    base = f"s3://{settings.s3_bucket}/inci_iceberg"
    catalog = INCIIceberg.get_catalog()

    # current (overwrite)
    current_table = _load_or_create_table(
        catalog,
        INCIIceberg.COSING_BRONZE_CURRENT_TABLE,
        f"{base}/cosing_bronze/current",
    )
    current_table.overwrite(_build_arrow_table(df, current_table))
    print(f"   Iceberg overwrite: {INCIIceberg.COSING_BRONZE_CURRENT_TABLE} ({len(df)}건)")

    # history (append)
    history_table = _load_or_create_table(
        catalog,
        INCIIceberg.COSING_BRONZE_HISTORY_TABLE,
        f"{base}/cosing_bronze/history",
    )
    history_table.append(_build_arrow_table(df, history_table))
    print(f"   Iceberg append:    {INCIIceberg.COSING_BRONZE_HISTORY_TABLE} ({len(df)}건)")
